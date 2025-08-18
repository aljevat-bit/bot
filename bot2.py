import os
import sys
import time
import json
import math
import queue
import sqlite3
import threading
import subprocess
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ---------------- Third-party ----------------
import pandas as pd
from dotenv import load_dotenv
import requests  # Import requests library for its exceptions

try:
    from binance.client import Client
    from binance.enums import *
    from binance.exceptions import BinanceAPIException
    from binance import ThreadedWebsocketManager
except ImportError:
    print("Dependencies not found. Please run: pip install python-binance python-dotenv pandas requests")
    sys.exit(1)

# ---------------- UI ----------------
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

###############################################################################
# Constants & Small Utils
###############################################################################

APP_CONFIG_FILE = os.path.expanduser("~/.simple_crypto_bot_config_v9.json")
DB_PATH_DEFAULT = os.path.expanduser("~/.simple_crypto_bot_trades_v9.sqlite")

INTERVAL_MINUTES = {
    Client.KLINE_INTERVAL_1MINUTE: 1, Client.KLINE_INTERVAL_3MINUTE: 3,
    Client.KLINE_INTERVAL_5MINUTE: 5, Client.KLINE_INTERVAL_15MINUTE: 15,
    Client.KLINE_INTERVAL_1HOUR: 60, Client.KLINE_INTERVAL_4HOUR: 240,
}


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_utc_iso(): return datetime.now(timezone.utc).isoformat()


def round_step(value, step):
    if not step or step == 0: return float(value)
    return float((Decimal(str(value)) // Decimal(str(step))) * Decimal(str(step)))


def safe_float(x, default=0.0):
    try:
        return float(x)
    except (ValueError, TypeError):
        return default


def human_money(x):
    try:
        return f"{safe_float(x):,.2f}"
    except (ValueError, TypeError):
        return str(x)


###############################################################################
# UNIFIED DATABASE MANAGER
###############################################################################

class TradeDB:
    def __init__(self, db_path, log_fn=print):
        self.log = log_fn
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self.lock, self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, mode TEXT, symbol TEXT, side TEXT, qty REAL, price REAL, quote_amount REAL, reason TEXT);")
            self.conn.execute("CREATE TABLE IF NOT EXISTS equity (ts TEXT PRIMARY KEY, equity REAL);")
            self.conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS market_data_fast (ts TEXT PRIMARY KEY, symbol TEXT, price REAL, ema_short REAL, ema_long REAL, rsi REAL);")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS market_data_hourly (ts TEXT PRIMARY KEY, symbol TEXT, open REAL, high REAL, low REAL, close REAL, avg_ema_short REAL, avg_ema_long REAL, avg_rsi REAL);")

    def set_meta(self, k, v):
        with self.lock, self.conn: self.conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)", (k, str(v)))

    def get_meta(self, k, default=None):
        with self.lock:
            cur = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,));
            row = cur.fetchone()
            return row[0] if row else default

    def record_trade(self, ts, mode, symbol, side, qty, price, quote_amount, reason):
        with self.lock, self.conn: self.conn.execute(
            "INSERT INTO trades (ts, mode, symbol, side, qty, price, quote_amount, reason) VALUES (?,?,?,?,?,?,?,?)",
            (ts, mode, symbol, side, safe_float(qty), safe_float(price), safe_float(quote_amount), reason))

    def record_equity(self, ts, equity):
        with self.lock, self.conn: self.conn.execute("INSERT OR REPLACE INTO equity (ts, equity) VALUES (?,?)",
                                                     (ts, safe_float(equity)))

    def log_tick(self, ts, symbol, price, indicators):
        with self.lock, self.conn:
            self.conn.execute("INSERT OR REPLACE INTO market_data_fast VALUES (?,?,?,?,?,?)",
                              (ts, symbol, price, indicators.get('ema_s'), indicators.get('ema_l'),
                               indicators.get('rsi')))

    def downsample_and_prune(self):
        with self.lock:
            try:
                self.log(f"[{now()}] Running hourly data aggregation and pruning...")
                cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=2)

                df = pd.read_sql_query(f"SELECT * FROM market_data_fast WHERE ts >= ?", self.conn,
                                       params=(cutoff_dt.isoformat(),))

                if df.empty:
                    self.log(f"[{now()}] No new data to aggregate.");
                    return

                df['ts'] = pd.to_datetime(df['ts'])
                df.set_index('ts', inplace=True)

                agg_rules = {
                    'price': ['first', 'max', 'min', 'last'],
                    'ema_short': 'mean', 'ema_long': 'mean', 'rsi': 'mean'
                }

                grouped = df.groupby('symbol')
                hourly_df = grouped.resample('1H').agg(agg_rules)
                hourly_df.columns = ['open', 'high', 'low', 'close', 'avg_ema_short', 'avg_ema_long', 'avg_rsi']
                hourly_df.reset_index(inplace=True)

                with self.conn:
                    for _, row in hourly_df.iterrows():
                        if pd.isna(row['open']): continue
                        self.conn.execute("INSERT OR REPLACE INTO market_data_hourly VALUES (?,?,?,?,?,?,?,?,?)",
                                          (row['ts'].isoformat(), row['symbol'], row['open'], row['high'],
                                           row['low'], row['close'], row['avg_ema_short'],
                                           row['avg_ema_long'], row['avg_rsi']))

                prune_cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
                with self.conn:
                    cur = self.conn.execute("DELETE FROM market_data_fast WHERE ts < ?", (prune_cutoff,))
                    self.log(f"[{now()}] Aggregation complete. Pruned {cur.rowcount} old records from fast data table.")
            except Exception as e:
                self.log(f"[{now()}] ERROR during data downsampling: {e}\n{traceback.format_exc()}")


###############################################################################
# Binance Wrapper
###############################################################################

class BinanceWrapper:
    def __init__(self, api_key, api_secret, use_testnet=True, log_fn=print):
        self.log = log_fn
        self.client = Client(api_key, api_secret, testnet=use_testnet, requests_params={'timeout': 20})
        if use_testnet: self.client.API_URL = 'https://testnet.binance.vision/api'
        self.price_feed = None

    def _retry(self, fn, *args, **kwargs):
        for attempt in range(6):
            try:
                return fn(*args, **kwargs)
            except BinanceAPIException as e:
                if (e.code in [-1003, -1015] or "Too many requests" in str(e)) and attempt < 5:
                    delay = min(8.0, 0.5 * (2 ** attempt));
                    self.log(f"[{now()}] Rate limit. Backing off {delay:.2f}s...");
                    time.sleep(delay);
                    continue
                self.log(f"[{now()}] Unhandled Binance API Error: {e}");
                raise
            except requests.exceptions.RequestException as e:
                if attempt < 5:
                    delay = min(8.0, 0.5 * (2 ** attempt));
                    self.log(f"[{now()}] Network error ({type(e).__name__}). Retrying in {delay:.2f}s...");
                    time.sleep(delay);
                    continue
                self.log(f"[{now()}] Critical Network Error after multiple retries: {e}");
                raise

    def get_klines_df(self, symbol, interval=Client.KLINE_INTERVAL_5MINUTE, limit=250):
        raw = self._retry(self.client.get_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(raw,
                          columns=['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'close_time', 'qa_vol', 'trades',
                                   'tb_base_vol', 'tb_quote_vol', 'ignore'], dtype=float)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms');
        df.set_index('ts', inplace=True)
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]

    def get_symbol_filters(self, symbol):
        if not hasattr(self, '_exchange_info'): self._exchange_info = self._retry(self.client.get_exchange_info)
        for s in self._exchange_info['symbols']:
            if s['symbol'] == symbol:
                filters = {f['filterType']: f for f in s['filters']}
                return {'lot_step': safe_float(filters.get('LOT_SIZE', {}).get('stepSize')),
                        'tick_size': safe_float(filters.get('PRICE_FILTER', {}).get('tickSize')),
                        'min_notional': safe_float(filters.get('NOTIONAL', {}).get('minNotional', 10.0))}
        return {}

    def market_buy_by_quote(self, symbol, quote_amount):
        f = self.get_symbol_filters(symbol)
        if quote_amount < f.get('min_notional', 10.0): raise ValueError(
            f"Amount {quote_amount} < minNotional {f.get('min_notional', 10.0)}")
        return self._retry(self.client.create_order, symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET,
                           quoteOrderQty=f"{quote_amount:.8f}")

    def market_sell_base_qty(self, symbol, base_qty):
        f = self.get_symbol_filters(symbol);
        qty = round_step(base_qty, f.get('lot_step'))
        price = self.price_feed.get_price(symbol) if self.price_feed else 0
        if (qty * price) < f.get('min_notional', 10.0): self.log(
            f"[{now()}] WARN: Sell for {symbol} may be below minNotional. Qty={qty} Price={price}")
        return self._retry(self.client.create_order, symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET,
                           quantity=f"{qty:.8f}")


###############################################################################
# Indicators & Strategies
###############################################################################

def ema(s: pd.Series, span: int) -> pd.Series: return s.ewm(span=span, adjust=False).mean()


def rsi(s: pd.Series, period: int = 14) -> pd.Series:
    delta = s.diff();
    gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
    avg_gain, avg_loss = gain.ewm(alpha=1 / period, adjust=False).mean(), loss.ewm(alpha=1 / period,
                                                                                   adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-12);
    return 100 - (100 / (1 + rs))


# --- STRATEGY 1: Original EMA + RSI ---
class EMARSI_Strategy:
    def __init__(self, aggressiveness=0, short=20, long=50, rsi_period=14, rsi_buy_max=55, rsi_sell_min=65, **kwargs):
        self.log = print
        self.aggressiveness = aggressiveness / 100.0
        self.short, self.long = short, long
        self.rsi_period, self.base_rsi_buy_max, self.rsi_sell_min = rsi_period, rsi_buy_max, rsi_sell_min

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy();
        out['EMA_S'], out['EMA_L'] = ema(out['Close'], self.short), ema(out['Close'], self.long);
        out['RSI'] = rsi(out['Close'], self.rsi_period)
        return out

    def get_signal_and_rank(self, df: pd.DataFrame):
        if len(df) < max(self.long, self.short) + 10: return 'HOLD', 0.0, {}
        is_bullish = df['EMA_S'].iloc[-2] > df['EMA_L'].iloc[-2]
        lookback = int(1 + (5 * self.aggressiveness))
        recent_cross = any(
            df['EMA_S'].iloc[-i] > df['EMA_L'].iloc[-i] and df['EMA_S'].iloc[-i - 1] <= df['EMA_L'].iloc[-i - 1] for i
            in range(2, lookback + 2))
        ema_condition = is_bullish if self.aggressiveness > 0.5 else recent_cross
        effective_rsi_max = self.base_rsi_buy_max + (self.aggressiveness * (70 - self.base_rsi_buy_max))
        rsi_val = df['RSI'].iloc[-2]
        is_rsi_ok = rsi_val <= effective_rsi_max
        # FIX: Cast numpy types to native python types for cleaner logging
        details = {
            'ema_s': float(df['EMA_S'].iloc[-2]), 'ema_l': float(df['EMA_L'].iloc[-2]),
            'rsi': float(rsi_val), 'ema_ok': bool(ema_condition), 'rsi_ok': bool(is_rsi_ok),
            'effective_rsi_max': float(effective_rsi_max)
        }
        if ema_condition and is_rsi_ok:
            rank = ((df['EMA_S'].iloc[-2] - df['EMA_L'].iloc[-2]) / df['Close'].iloc[-2]) * 100 + (
                        self.rsi_sell_min - rsi_val)
            return 'BUY', rank, details
        is_crossunder = df['EMA_S'].iloc[-2] < df['EMA_L'].iloc[-2] and df['EMA_S'].iloc[-3] >= df['EMA_L'].iloc[-3]
        if is_crossunder or rsi_val >= self.rsi_sell_min: return 'SELL', 0.0, details
        return 'HOLD', 0.0, details


# --- STRATEGY 2: Candlestick Enhanced Trend Following ---
class CandlestickTrendStrategy:
    def __init__(self, short=50, long=200, rsi_period=14, **kwargs):
        self.log = print
        self.short = short
        self.long = long
        self.rsi_period = rsi_period

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out['EMA_S'] = ema(out['Close'], self.short)
        out['EMA_L'] = ema(out['Close'], self.long)
        out['RSI'] = rsi(out['Close'], self.rsi_period)
        return out

    def _is_bullish_engulfing(self, df: pd.DataFrame) -> bool:
        if len(df) < 2: return False
        last, prev = df.iloc[-1], df.iloc[-2]
        if not (prev['Open'] > prev['Close'] and last['Close'] > last['Open']): return False
        if last['Close'] > prev['Open'] and last['Open'] < prev['Close']: return True
        return False

    def _is_bearish_engulfing(self, df: pd.DataFrame) -> bool:
        if len(df) < 2: return False
        last, prev = df.iloc[-1], df.iloc[-2]
        if not (prev['Close'] > prev['Open'] and last['Open'] > last['Close']): return False
        if last['Open'] > prev['Close'] and last['Close'] < prev['Open']: return True
        return False

    def _is_hammer(self, df: pd.DataFrame) -> bool:
        if len(df) < 4: return False
        is_downtrend = df['Close'].iloc[-4] > df['Close'].iloc[-3] > df['Close'].iloc[-2]
        if not is_downtrend: return False
        last = df.iloc[-1]
        body_size = abs(last['Close'] - last['Open'])
        if body_size == 0: return False
        lower_shadow = (last['Open'] if last['Close'] > last['Open'] else last['Close']) - last['Low']
        upper_shadow = last['High'] - (last['Close'] if last['Close'] > last['Open'] else last['Open'])
        return lower_shadow > (body_size * 2) and upper_shadow < body_size

    def _is_dark_cloud_cover(self, df: pd.DataFrame) -> bool:
        if len(df) < 2: return False
        last, prev = df.iloc[-1], df.iloc[-2]
        if not (prev['Close'] > prev['Open'] and last['Open'] > last['Close']): return False
        midpoint_prev_body = (prev['Open'] + prev['Close']) / 2
        if last['Open'] > prev['High'] and last['Close'] < midpoint_prev_body: return True
        return False

    def get_signal_and_rank(self, df: pd.DataFrame):
        if len(df) < max(self.long, self.short) + 10: return 'HOLD', 0.0, {}

        # FIX: Call each pattern check only once for efficiency
        candle_df = df.iloc[-3:-1]
        is_bull_engulf = self._is_bullish_engulfing(candle_df)
        is_bear_engulf = self._is_bearish_engulfing(candle_df)
        is_hammer_pattern = self._is_hammer(candle_df)
        is_dark_cloud = self._is_dark_cloud_cover(candle_df)

        is_bearish_reversal = is_bear_engulf or is_dark_cloud
        is_bullish_reversal = is_bull_engulf or is_hammer_pattern

        # Determine overall trend from the full dataframe
        is_uptrend = df['EMA_S'].iloc[-1] > df['EMA_L'].iloc[-1]

        bearish_pattern = "Engulfing" if is_bear_engulf else "Dark Cloud" if is_dark_cloud else "None"
        bullish_pattern = "Engulfing" if is_bull_engulf else "Hammer" if is_hammer_pattern else "None"

        # FIX: Cast numpy types to native python types for cleaner logging
        details = {
            'ema_s': float(df['EMA_S'].iloc[-2]), 'ema_l': float(df['EMA_L'].iloc[-2]),
            'rsi': float(df['RSI'].iloc[-2]), 'is_uptrend': bool(is_uptrend),
            'bearish_pattern': bearish_pattern, 'bullish_pattern': bullish_pattern
        }

        if is_bearish_reversal:
            self.log(f"[{now()}] Strategy found Bearish Pattern: {bearish_pattern}")
            return 'SELL', 100.0, details

        if is_uptrend and is_bullish_reversal and df['RSI'].iloc[-2] < 80:
            self.log(f"[{now()}] Strategy found Bullish Pattern in Uptrend: {bullish_pattern}")
            rank = 80 - df['RSI'].iloc[-2]
            return 'BUY', rank, details

        return 'HOLD', 0.0, details


###############################################################################
# Paper Broker & Live Price Feed
###############################################################################

class PaperBroker:
    def __init__(self, price_feed, db, starting_balance=10000.0, log_fn=print):
        self.price_feed, self.db, self.log = price_feed, db, log_fn
        self.cash = safe_float(self.db.get_meta("paper_cash", starting_balance))
        if self.cash == 0: self.cash = starting_balance
        self.db.set_meta("paper_cash", self.cash)

    def market_buy_by_quote(self, symbol, quote_amount):
        if quote_amount < 10.0: raise ValueError("Amount < 10.0 minNotional")
        price = self.price_feed.get_price(symbol)
        if price is None: raise ValueError(f"No live price for {symbol} to execute paper trade.")
        base_qty = quote_amount / price;
        cost = base_qty * price
        if cost > self.cash: raise ValueError(f"Insufficient paper cash: need {cost:.2f}, have {self.cash:.2f}")
        self.cash -= cost;
        self.db.set_meta("paper_cash", self.cash)
        self.db.record_trade(now(), "paper", symbol, "BUY", base_qty, price, cost, "paper_buy")
        return {"fills": [{"qty": str(base_qty), "price": str(price)}]}

    def market_sell_base_qty(self, symbol, base_qty):
        price = self.price_feed.get_price(symbol)
        if price is None: raise ValueError(f"No live price for {symbol} to execute paper trade.")
        proceeds = base_qty * price;
        self.cash += proceeds;
        self.db.set_meta("paper_cash", self.cash)
        self.db.record_trade(now(), "paper", symbol, "SELL", base_qty, price, proceeds, "paper_sell")
        return {"fills": [{"qty": str(base_qty), "price": str(price)}]}


class LivePriceFeed(threading.Thread):
    def __init__(self, symbols, use_testnet=False, log_fn=print):
        super().__init__(daemon=True)
        self.symbols, self.use_testnet, self.log = list(symbols), use_testnet, log_fn
        self._prices, self._lock, self._running, self._twm = {}, threading.Lock(), threading.Event(), None

    def get_price(self, symbol):
        with self._lock: return self._prices.get(symbol)

    def stop(self):
        self._running.clear()

    def run(self):
        self._running.set()
        try:
            self._twm = ThreadedWebsocketManager(testnet=self.use_testnet)
            self._twm.start()
            streams = [s.lower() + '@miniTicker' for s in self.symbols]
            self._twm.start_multiplex_socket(callback=self._ws_cb, streams=streams)
            self.log(f"[{now()}] WebSocket price feed started for {len(self.symbols)} symbols.")
            while self._running.is_set(): time.sleep(1)
        except Exception as e:
            self.log(f"[{now()}] FATAL ERROR in LivePriceFeed thread: {e}\n{traceback.format_exc()}")
        finally:
            if self._twm: self._twm.stop()
            self.log(f"[{now()}] WebSocket price feed stopped.")

    def _ws_cb(self, msg):
        try:
            if msg and 'data' in msg:
                sym, p = msg['data'].get('s'), safe_float(msg['data'].get('c'))
                if sym and p:
                    with self._lock: self._prices[sym] = p
        except Exception:
            pass


###############################################################################
# Bot Engine
###############################################################################

class BotEngine(threading.Thread):
    def __init__(self, data_client, order_client, price_feed, mode, symbols_cfg,
                 strategy_choice, strategy_cfg, risk_cfg, timeframe, db_manager, ui_logger, ui_updater):
        super().__init__(daemon=True)
        self.log, self.ui_updater = ui_logger, ui_updater
        self.data, self.order, self.price_feed = data_client, order_client, price_feed
        self.mode, self.symbols_cfg, self.timeframe, self.db = mode, symbols_cfg, timeframe, db_manager
        self.running = threading.Event()

        if strategy_choice == "CANDLESTICK":
            self.log(f"[{now()}] Using Candlestick + Trend Strategy.")
            self.strategy = CandlestickTrendStrategy(**strategy_cfg)
        else:
            self.log(f"[{now()}] Using EMA + RSI Strategy.")
            self.strategy = EMARSI_Strategy(**strategy_cfg)
        self.strategy.log = self.log

        self.sl_pct, self.tp_pct = safe_float(risk_cfg.get('stop_loss_pct', 2.0)) / 100.0, safe_float(
            risk_cfg.get('take_profit_pct', 3.0)) / 100.0
        self.max_total_pos = int(risk_cfg.get('max_total_positions', 5))
        self.max_pos_per_coin = int(risk_cfg.get('max_pos_per_coin', 1))
        self.starting_equity = safe_float(risk_cfg.get('starting_equity', 10000.0))

        self.positions = {}
        self.next_fetch_ts, self.last_candle_ts = {s: 0 for s in self.symbols_cfg}, {s: None for s in self.symbols_cfg}
        self.next_pos_id = 1

    def stop(self):
        self.running.clear()

    def _get_total_open_positions(self):
        return sum(len(pos_list) for pos_list in self.positions.values())

    def _update_ui(self, equity, peak, drawdown):
        if not self.ui_updater: return
        flat_pos_list = []
        for sym, pos_list in self.positions.items():
            for pos in pos_list:
                price = self.price_feed.get_price(sym) or pos['entry']
                pnl_pct = ((price - pos['entry']) / pos['entry']) * 100.0 if pos['entry'] else 0.0
                flat_pos_list.append(
                    {**pos, 'symbol': sym, 'last_price': price, 'pnl_pct': pnl_pct, 'value': pos['qty'] * price})
        self.ui_updater({'positions': flat_pos_list, 'equity': equity, 'peak': peak, 'drawdown': drawdown})

    def _eval_equity(self):
        cash = self.order.cash if isinstance(self.order, PaperBroker) else 0.0
        asset_val = sum(
            pos['qty'] * (self.price_feed.get_price(sym) or pos['entry']) for sym, pos_list in self.positions.items()
            for pos in pos_list)
        return cash + asset_val

    def _market_exit(self, symbol, pos_id, reason=""):
        pos_list = self.positions.get(symbol, [])
        pos_to_close = next((p for p in pos_list if p['id'] == pos_id), None)
        if not pos_to_close: return
        try:
            self.order.market_sell_base_qty(symbol, pos_to_close['qty'])
            self.log(f"[{now()}] ✅ SELL {symbol} (ID: {pos_id}): {pos_to_close['qty']:.6f} @ market | reason={reason}")
            pos_list.remove(pos_to_close)
            if not pos_list: del self.positions[symbol]
        except Exception as e:
            self.log(f"[{now()}] ❌ SELL FAILED for {symbol} (ID: {pos_id}): {e}")

    def run(self):
        self.running.set()
        self.log(f"[{now()}] Bot started. Mode={self.mode}, TF={self.timeframe}")
        equity, peak_equity = self.starting_equity, self.starting_equity
        timers = {'ui_update': 0, 'scan': 0, 'downsample': 0}

        while self.running.is_set():
            now_ts = time.time()
            try:
                for sym, pos_list in list(self.positions.items()):
                    price = self.price_feed.get_price(sym)
                    if price:
                        for pos in list(pos_list):
                            if pos.get('sl') and price <= pos['sl']:
                                self._market_exit(sym, pos['id'], "stop_loss")
                            elif pos.get('tp') and price >= pos['tp']:
                                self._market_exit(sym, pos['id'], "take_profit")

                if now_ts - timers['ui_update'] > 1.0:
                    equity, peak_equity = self._eval_equity(), max(peak_equity, equity)
                    self._update_ui(equity, peak_equity, (1 - equity / peak_equity) if peak_equity else 0)
                    self.db.record_equity(datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S.%f"), equity)
                    timers['ui_update'] = now_ts

                for sym in self.symbols_cfg.keys():
                    if now_ts > self.next_fetch_ts.get(sym, 0):
                        try:
                            df = self.data.get_klines_df(sym, self.timeframe, 250)
                            candle_ts = df.index[-2]
                            if self.last_candle_ts.get(sym) != candle_ts:
                                self.last_candle_ts[sym] = candle_ts
                                df_ind = self.strategy.compute(df)
                                signal, _, details = self.strategy.get_signal_and_rank(df_ind)
                                self.db.log_tick(now_utc_iso(), sym, df['Close'].iloc[-1], details)
                                if sym in self.positions and signal == 'SELL':
                                    self.log(f"[{now()}] SELL Signal on {sym}. Closing all positions for this symbol.")
                                    for pos in list(self.positions.get(sym, [])): self._market_exit(sym, pos['id'],
                                                                                                    "signal")
                            self.next_fetch_ts[sym] = (candle_ts + timedelta(
                                minutes=INTERVAL_MINUTES.get(self.timeframe, 5))).timestamp()
                        except Exception as e:
                            self.log(f"[{now()}] Kline error for {sym}: {e}");
                            self.next_fetch_ts[sym] = now_ts + 30

                # --- Scan frequency increased to 5 seconds ---
                if self._get_total_open_positions() < self.max_total_pos and now_ts - timers['scan'] > 5:
                    self._scan_and_fill();
                    timers['scan'] = now_ts

                if now_ts - timers['downsample'] > 3600:
                    self.db.downsample_and_prune();
                    timers['downsample'] = now_ts

            except Exception as e:
                self.log(f"[{now()}] FATAL LOOP ERROR: {e}\n{traceback.format_exc()}")
            time.sleep(0.05)
        self.log(f"[{now()}] Bot stopped.")

    def _scan_and_fill(self):
        if self._get_total_open_positions() >= self.max_total_pos: return
        self.log(
            f"--- Scanning for new positions. Capacity: {self._get_total_open_positions()}/{self.max_total_pos} ---")
        candidates = []
        for sym in self.symbols_cfg.keys():
            open_count = len(self.positions.get(sym, []))
            if open_count >= self.max_pos_per_coin: continue
            if self._get_total_open_positions() + len(candidates) >= self.max_total_pos: break
            try:
                df_ind = self.strategy.compute(self.data.get_klines_df(sym, self.timeframe, 250))
                signal, rank, details = self.strategy.get_signal_and_rank(df_ind)

                # --- FIX: Cleaner logging logic ---
                if signal == 'BUY':
                    log_msg = f"  - [{sym}] ✅ BUY Signal. Rank: {rank:.2f}. Details: {details}"
                    self.log(log_msg)
                    candidates.append({'symbol': sym, 'rank': rank})
                # No detailed log here for HOLD/SELL, as this function is only for buying
            except Exception as e:
                self.log(f"  - [{sym}] ⚠️ ERROR analyzing symbol: {e}")

        if not candidates:
            self.log("--- Scan Complete: No new buy opportunities found. ---")
            return

        self.log("--- Scan Complete ---")
        candidates.sort(key=lambda x: x['rank'], reverse=True)
        for entry in candidates:
            if self._get_total_open_positions() >= self.max_total_pos: break
            self._market_enter(entry['symbol'])

    def _market_enter(self, symbol):
        total_amount = safe_float(self.symbols_cfg[symbol]['amount']);
        trade_amount = total_amount / self.max_pos_per_coin
        try:
            self.log(f"[{now()}] Attempting to BUY {symbol} with amount ~${trade_amount:.2f}")
            order = self.order.market_buy_by_quote(symbol, trade_amount)
            qty, cprice = 0, 0
            if 'fills' in order and order['fills']:
                tot_q = sum(safe_float(f['qty']) for f in order['fills']);
                tot_v = sum(safe_float(f['qty']) * safe_float(f['price']) for f in order['fills'])
                qty, cprice = tot_q, (tot_v / tot_q) if tot_q else (0, 0)
            elif 'cummulativeQuoteQty' in order:
                qty, cprice = safe_float(order['executedQty']), safe_float(order['cummulativeQuoteQty']) / safe_float(
                    order['executedQty']) if safe_float(order['executedQty']) > 0 else 0
            if qty > 0:
                pos_id = self.next_pos_id;
                self.next_pos_id += 1
                sl, tp = cprice * (1.0 - self.sl_pct), cprice * (1.0 + self.tp_pct)
                self.positions.setdefault(symbol, []).append(
                    {'id': pos_id, 'qty': qty, 'entry': cprice, 'sl': sl, 'tp': tp})
                self.log(f"[{now()}] ✅ BUY SUCCESSFUL {symbol} (ID: {pos_id}): {qty:.6f} @ {cprice:.4f}")
            else:
                self.log(f"[{now()}] ❌ BUY FAILED for {symbol}: Zero quantity in order response.")
        except Exception as e:
            self.log(f"[{now()}] ❌ BUY FAILED for {symbol}: {e}")


###############################################################################
# UI
###############################################################################

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Simple Crypto Bot v9")
        self.geometry("1450x850");
        self.minsize(1200, 700);
        load_dotenv()
        self.symbol_rows, self.ui_queue = {}, queue.Queue()
        self.bot_thread, self.price_feed = None, None

        self.var_strategy_choice = tk.StringVar(value="EMARSI")
        self.var_mode = tk.StringVar(value="paper");
        self.var_api_key = tk.StringVar(value=os.getenv("BINANCE_API_KEY", ""));
        self.var_api_secret = tk.StringVar(value=os.getenv("BINANCE_API_SECRET", ""))
        self.var_quote = tk.StringVar(value="USDT");
        self.var_interval = tk.StringVar(value=Client.KLINE_INTERVAL_5MINUTE)
        self.var_ema_short, self.var_ema_long = tk.IntVar(value=20), tk.IntVar(value=50);
        self.var_rsi_period = tk.IntVar(value=14)
        self.var_rsi_buy_max, self.var_rsi_sell_min = tk.DoubleVar(value=55.0), tk.DoubleVar(value=65.0)
        self.var_aggressiveness = tk.IntVar(value=0)
        self.var_sl, self.var_tp = tk.DoubleVar(value=2.0), tk.DoubleVar(value=3.0)
        self.var_max_total_pos = tk.IntVar(value=5);
        self.var_max_pos_per_coin = tk.IntVar(value=1)
        self.var_paper_start = tk.DoubleVar(value=10000.0)
        self.var_status = tk.StringVar(value="Idle");
        self.var_equity = tk.StringVar(value="Equity: 10,000.00")
        self.var_peak_equity = tk.StringVar(value="Peak: 10,000.00");
        self.var_drawdown = tk.StringVar(value="Drawdown: 0.00%")
        self.var_db_path = tk.StringVar(value=DB_PATH_DEFAULT)

        self._build_ui();
        self._load_config()
        self.db = TradeDB(self.var_db_path.get(), self.log)
        self.after(100, self._drain_ui_queue);
        self.after(1000, self._refresh_symbol_prices);
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _build_ui(self):
        main_pane = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left_frame = ttk.Frame(main_pane, width=450);
        right_frame = ttk.Frame(main_pane, width=800)
        main_pane.add(left_frame, weight=1);
        main_pane.add(right_frame, weight=3)
        self._build_left_pane(left_frame);
        self._build_right_pane(right_frame)

    def _build_left_pane(self, parent):
        self._build_equity_bar(parent);
        self._build_top(parent);
        self._build_symbols(parent)
        self._build_strategy(parent);
        self._build_risk(parent);
        self._build_controls(parent)

    def _build_right_pane(self, parent):
        self._build_positions_table(parent);
        self._build_log(parent)

    def _build_equity_bar(self, p):
        frm = ttk.LabelFrame(p, text="Live Performance");
        frm.pack(fill="x", padx=10, pady=5)
        ttk.Label(frm, textvariable=self.var_equity).pack(side="left", padx=10, pady=5);
        ttk.Label(frm, textvariable=self.var_peak_equity).pack(side="left", padx=10, pady=5);
        ttk.Label(frm, textvariable=self.var_drawdown).pack(side="left", padx=10, pady=5)

    def _build_top(self, p):
        frm = ttk.LabelFrame(p, text="Mode & Credentials");
        frm.pack(fill="x", padx=10, pady=5)
        f_mode = ttk.Frame(frm);
        f_mode.pack(fill='x', pady=2)
        ttk.Label(f_mode, text="Mode:").pack(side="left", padx=5)
        for txt, val in [("Paper", "paper"), ("Testnet", "testnet"), ("Live", "live")]: ttk.Radiobutton(f_mode,
                                                                                                        text=txt,
                                                                                                        variable=self.var_mode,
                                                                                                        value=val).pack(
            side="left")
        f_keys = ttk.Frame(frm);
        f_keys.pack(fill='x', pady=2)
        ttk.Label(f_keys, text="API Key").pack(side="left", padx=5);
        ttk.Entry(f_keys, textvariable=self.var_api_key, width=40).pack(side="left")
        f_sec = ttk.Frame(frm);
        f_sec.pack(fill='x', pady=2)
        ttk.Label(f_sec, text="API Secret").pack(side="left", padx=5);
        ttk.Entry(f_sec, textvariable=self.var_api_secret, show="•", width=40).pack(side="left")
        f_data = ttk.Frame(frm);
        f_data.pack(fill='x', pady=2)
        ttk.Label(f_data, text="Quote:").pack(side="left", padx=5);
        ttk.OptionMenu(f_data, self.var_quote, "USDT", "USDT", "USDC").pack(side="left")
        ttk.Label(f_data, text="Timeframe:").pack(side="left", padx=15)
        ttk.OptionMenu(f_data, self.var_interval, self.var_interval.get(),
                       *[v for k, v in Client.__dict__.items() if k.startswith('KLINE_INTERVAL')]).pack(side="left")

    def _build_symbols(self, p):
        frm = ttk.LabelFrame(p, text="Symbols & Total Amount Allocation");
        frm.pack(fill="x", padx=10, pady=5)
        self.symbol_frame = frm
        f_header = ttk.Frame(frm);
        f_header.pack(fill='x', padx=5, pady=2)
        ttk.Label(f_header, text="Base").pack(side="left");
        ttk.Label(f_header, text="Total Amount").pack(side="right");
        ttk.Label(f_header, text="Live Price").pack(side="right", padx=20)
        self.add_symbol_row("BTC", 200.0);
        self.add_symbol_row("ETH", 100.0)
        f_new = ttk.Frame(frm);
        f_new.pack(fill='x', pady=5)
        self.new_symbol_var = tk.StringVar()
        ttk.Entry(f_new, textvariable=self.new_symbol_var, width=12).pack(side="left", padx=5);
        ttk.Button(f_new, text="Add Symbol", command=self._add_symbol_from_entry).pack(side="left")

    def add_symbol_row(self, base, amount):
        base = base.upper()
        if base in self.symbol_rows: return
        frm = ttk.Frame(self.symbol_frame);
        frm.pack(fill='x', padx=5, pady=1)
        amt_var = tk.StringVar(value=str(amount));
        price_var = tk.StringVar(value="--.--")
        lbl = ttk.Label(frm, text=base, width=8);
        lbl.pack(side="left")
        ent = ttk.Entry(frm, textvariable=amt_var, width=10);
        ent.pack(side="right")
        price_lbl = ttk.Label(frm, textvariable=price_var, width=15, anchor='e');
        price_lbl.pack(side="right", padx=10)

        def remove(): frm.destroy(); self.symbol_rows.pop(base, None)

        btn = ttk.Button(frm, text="Remove", command=remove);
        btn.pack(side="left", padx=5)
        self.symbol_rows[base] = {'amount_var': amt_var, 'price_var': price_var, 'frame': frm}

    def _add_symbol_from_entry(self):
        base = self.new_symbol_var.get().strip().upper()
        if base: self.add_symbol_row(base, 50.0)
        self.new_symbol_var.set("")

    def _refresh_symbol_prices(self):
        if self.price_feed and self.price_feed.is_alive():
            for base, row in self.symbol_rows.items():
                price = self.price_feed.get_price(base + self.var_quote.get())
                if price: row['price_var'].set(f"{price:,.4f}")
        self.after(1000, self._refresh_symbol_prices)

    def _build_strategy(self, p):
        frm = ttk.LabelFrame(p, text="Strategy");
        frm.pack(fill="x", padx=10, pady=5)

        f_choice = ttk.Frame(frm);
        f_choice.pack(fill='x', pady=2, padx=5)
        ttk.Label(f_choice, text="Active Strategy:").pack(side="left")
        ttk.Radiobutton(f_choice, text="EMA + RSI", variable=self.var_strategy_choice, value="EMARSI").pack(side="left",
                                                                                                            padx=5)
        ttk.Radiobutton(f_choice, text="Candlestick + Trend", variable=self.var_strategy_choice,
                        value="CANDLESTICK").pack(side="left", padx=5)

        ttk.Separator(frm, orient='horizontal').pack(fill='x', pady=5, padx=5)

        f_aggr = ttk.Frame(frm);
        f_aggr.pack(fill='x', pady=2, padx=5)
        self.aggressiveness_label = ttk.Label(f_aggr, text="Aggressiveness (EMA+RSI only): Conservative (0)")
        self.aggressiveness_label.pack(side="left")

        def update_slider_label(val):
            v = int(float(val));
            label = "Conservative"
            if 30 <= v < 70:
                label = "Balanced"
            elif v >= 70:
                label = "Aggressive"
            self.aggressiveness_label.config(text=f"Aggressiveness (EMA+RSI only): {label} ({v})")

        slider = ttk.Scale(f_aggr, from_=0, to=100, orient='horizontal', variable=self.var_aggressiveness,
                           command=update_slider_label)
        slider.pack(side="left", fill='x', expand=True, padx=10)

        f1 = ttk.Frame(frm);
        f1.pack(fill='x')
        ttk.Label(f1, text="EMA Short").pack(side="left", padx=5);
        ttk.Spinbox(f1, from_=3, to=200, textvariable=self.var_ema_short, width=5).pack(side="left")
        ttk.Label(f1, text="EMA Long").pack(side="left", padx=15);
        ttk.Spinbox(f1, from_=5, to=400, textvariable=self.var_ema_long, width=5).pack(side="left")
        f2 = ttk.Frame(frm);
        f2.pack(fill='x', pady=2)
        ttk.Label(f2, text="RSI Period").pack(side="left", padx=5);
        ttk.Spinbox(f2, from_=5, to=50, textvariable=self.var_rsi_period, width=5).pack(side="left")
        ttk.Label(f2, text="Base RSI Buy ≤").pack(side="left", padx=15);
        ttk.Spinbox(f2, from_=10, to=70, textvariable=self.var_rsi_buy_max, width=5).pack(side="left")
        ttk.Label(f2, text="RSI Sell ≥").pack(side="left", padx=15);
        ttk.Spinbox(f2, from_=30, to=90, textvariable=self.var_rsi_sell_min, width=5).pack(side="left")

    def _build_risk(self, p):
        frm = ttk.LabelFrame(p, text="Risk & Position Controls");
        frm.pack(fill="x", padx=10, pady=5)
        f1 = ttk.Frame(frm);
        f1.pack(fill='x')
        ttk.Label(f1, text="Stop Loss %").pack(side="left", padx=5);
        ttk.Spinbox(f1, from_=0.2, to=20, increment=0.1, textvariable=self.var_sl, width=5).pack(side="left")
        ttk.Label(f1, text="Take Profit %").pack(side="left", padx=15);
        ttk.Spinbox(f1, from_=0.2, to=50, increment=0.1, textvariable=self.var_tp, width=5).pack(side="left")
        f2 = ttk.Frame(frm);
        f2.pack(fill='x', pady=2)
        ttk.Label(f2, text="Max Total Pos").pack(side="left", padx=5);
        ttk.Spinbox(f2, from_=1, to=20, textvariable=self.var_max_total_pos, width=5).pack(side="left")
        ttk.Label(f2, text="Max Pos / Coin").pack(side="left", padx=15);
        ttk.Spinbox(f2, from_=1, to=10, textvariable=self.var_max_pos_per_coin, width=5).pack(side="left")
        f3 = ttk.Frame(frm);
        f3.pack(fill='x', pady=2)
        ttk.Label(f3, text="Paper Start Equity").pack(side="left", padx=5);
        ttk.Entry(f3, textvariable=self.var_paper_start, width=10).pack(side="left")

    def _build_controls(self, p):
        frm = ttk.Frame(p);
        frm.pack(fill="x", padx=10, pady=10)
        self.btn_start = ttk.Button(frm, text="Start Bot", command=self._start_bot);
        self.btn_start.pack(side="left", padx=5)
        self.btn_stop = ttk.Button(frm, text="Stop Bot", command=self._stop_bot, state="disabled");
        self.btn_stop.pack(side="left", padx=5)
        ttk.Button(frm, text="Save Config", command=self._save_config).pack(side="left", padx=5);
        ttk.Label(frm, textvariable=self.var_status).pack(side="right", padx=10)

    def _build_positions_table(self, p):
        frm = ttk.LabelFrame(p, text="Open Positions");
        frm.pack(fill="x", padx=5, pady=5)
        cols = ('id', 'symbol', 'qty', 'entry', 'value', 'pnl_pct', 'sl', 'tp', 'last_price')
        self.tree = ttk.Treeview(frm, columns=cols, show='headings', height=8);
        self.tree.pack(side="left", fill="x", expand=True)
        vsb = ttk.Scrollbar(frm, orient="vertical", command=self.tree.yview);
        vsb.pack(side='right', fill='y');
        self.tree.configure(yscrollcommand=vsb.set)
        col_map = {"id": ("ID", 40), "symbol": ("Symbol", 80), "qty": ("Qty", 100), "entry": ("Entry", 80),
                   "value": ("Value", 80), "pnl_pct": ("PnL %", 60), "sl": ("Stop Loss", 80), "tp": ("Take Profit", 80),
                   "last_price": ("Last Price", 80)}
        for c in cols: self.tree.heading(c, text=col_map[c][0]); self.tree.column(c, width=col_map[c][1],
                                                                                  anchor='center')

    def _build_log(self, p):
        frm = ttk.LabelFrame(p, text="Log");
        frm.pack(fill="both", expand=True, padx=5, pady=5)
        self.txt = tk.Text(frm, height=10, bg="#f0f0f0", fg="black");
        self.txt.pack(side="left", fill="both", expand=True)
        vsb = ttk.Scrollbar(frm, orient="vertical", command=self.txt.yview);
        vsb.pack(side='right', fill='y');
        self.txt.configure(yscrollcommand=vsb.set)

    def log(self, msg):
        self.ui_queue.put({'type': 'log', 'data': str(msg)})

    def _ui_updater_callback(self, data):
        self.ui_queue.put({'type': 'update', 'data': data})

    def _drain_ui_queue(self):
        try:
            while not self.ui_queue.empty():
                item = self.ui_queue.get_nowait()
                if item['type'] == 'log':
                    self.txt.insert("end", item['data'] + "\n"); self.txt.see("end")
                elif item['type'] == 'update':
                    d = item['data'];
                    self.var_equity.set(f"Equity: {human_money(d['equity'])}");
                    self.var_peak_equity.set(f"Peak: {human_money(d['peak'])}");
                    self.var_drawdown.set(f"Drawdown: {d['drawdown']:.2%}")
                    self.tree.delete(*self.tree.get_children())
                    for p in sorted(d['positions'], key=lambda x: x['id']):
                        self.tree.insert("", "end",
                                         values=(p['id'], p['symbol'], f"{p['qty']:.6f}", f"{p['entry']:.4f}",
                                                 human_money(p['value']), f"{p['pnl_pct']:.2f}%", f"{p['sl']:.4f}",
                                                 f"{p['tp']:.4f}", f"{p['last_price']:.4f}"))
        except queue.Empty:
            pass
        finally:
            self.after(100, self._drain_ui_queue)

    def _get_configs(self):
        symbols_cfg = {base.upper() + self.var_quote.get(): {'amount': row['amount_var'].get()} for base, row in
                       self.symbol_rows.items()}
        strategy_cfg = {'aggressiveness': self.var_aggressiveness.get(), 'short': self.var_ema_short.get(),
                        'long': self.var_ema_long.get(), 'rsi_period': self.var_rsi_period.get(),
                        'rsi_buy_max': self.var_rsi_buy_max.get(), 'rsi_sell_min': self.var_rsi_sell_min.get()}
        risk_cfg = {'stop_loss_pct': self.var_sl.get(), 'take_profit_pct': self.var_tp.get(),
                    'max_total_positions': self.var_max_total_pos.get(),
                    'max_pos_per_coin': self.var_max_pos_per_coin.get(), 'starting_equity': self.var_paper_start.get()}
        return symbols_cfg, strategy_cfg, risk_cfg

    def _start_bot(self):
        mode = self.var_mode.get()
        symbols_cfg, strategy_cfg, risk_cfg = self._get_configs()
        if not symbols_cfg: messagebox.showerror("Error", "Add at least one symbol."); return

        self.db = TradeDB(self.var_db_path.get(), self.log)
        all_symbols = list(symbols_cfg.keys())
        self.price_feed = LivePriceFeed(all_symbols, use_testnet=(mode == "testnet"), log_fn=self.log)
        self.price_feed.start()
        self.log(f"[{now()}] Waiting for price feed to establish connection...")
        connection_wait_seconds, connection_established = 10, False
        for i in range(connection_wait_seconds):
            if any(self.price_feed.get_price(s) for s in all_symbols): self.log(
                f"[{now()}] Price feed is live after {i + 1} second(s)."); connection_established = True; break
            time.sleep(1)
        if not connection_established:
            self.log(f"[{now()}] ERROR: Price feed failed after {connection_wait_seconds}s. Check connection/symbols.")
            if self.price_feed: self.price_feed.stop()
            return

        try:
            data_client = BinanceWrapper(None, None, (mode != "live"), log_fn=self.log);
            data_client.price_feed = self.price_feed
            if mode == "paper":
                order_client = PaperBroker(self.price_feed, self.db, risk_cfg['starting_equity'], self.log)
            else:
                key, sec = self.var_api_key.get().strip(), self.var_api_secret.get().strip()
                if not key or not sec: messagebox.showerror("Keys required",
                                                            f"Enter API key/secret for {mode} mode."); return
                order_client = BinanceWrapper(key, sec, (mode == "testnet"), self.log);
                order_client.price_feed = self.price_feed
        except Exception as e:
            messagebox.showerror("Connection Failed", str(e)); return

        strategy_choice = self.var_strategy_choice.get()
        self.bot_thread = BotEngine(data_client, order_client, self.price_feed, mode, symbols_cfg,
                                    strategy_choice, strategy_cfg, risk_cfg, self.var_interval.get(),
                                    self.db, self.log, self._ui_updater_callback)
        self.bot_thread.start()
        self.btn_start.config(state="disabled");
        self.btn_stop.config(state="normal");
        self.var_status.set(f"Running ({mode})")

    def _stop_bot(self):
        if self.bot_thread: self.bot_thread.stop(); self.bot_thread.join(5)
        if self.price_feed: self.price_feed.stop(); self.price_feed.join(2)
        self.bot_thread, self.price_feed = None, None
        self.btn_start.config(state="normal");
        self.btn_stop.config(state="disabled");
        self.var_status.set("Stopped")

    def _on_closing(self):
        if self.bot_thread and self.bot_thread.is_alive():
            if messagebox.askyesno("Exit",
                                   "Bot is running. Are you sure you want to exit?"): self._stop_bot(); self.destroy()
        else:
            self.destroy()

    def _save_config(self):
        symbols = {base: row['amount_var'].get() for base, row in self.symbol_rows.items()}
        s_cfg, r_cfg = self._get_configs()[1:]
        s_cfg['aggressiveness'] = self.var_aggressiveness.get()

        cfg = {'mode': self.var_mode.get(), 'api_key': self.var_api_key.get(), 'api_secret': self.var_api_secret.get(),
               'quote': self.var_quote.get(), 'interval': self.var_interval.get(), 'symbols': symbols,
               'strategy_choice': self.var_strategy_choice.get(), 'strategy': s_cfg, 'risk': r_cfg,
               'db_path': self.var_db_path.get()}
        try:
            with open(APP_CONFIG_FILE, "w") as f:
                json.dump(cfg, f, indent=2)
            self.log(f"[{now()}] Config saved to {APP_CONFIG_FILE}")
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

    def _load_config(self):
        if not os.path.isfile(APP_CONFIG_FILE): return
        try:
            with open(APP_CONFIG_FILE, "r") as f:
                cfg = json.load(f)
            self.var_mode.set(cfg.get('mode', "paper"));
            self.var_api_key.set(cfg.get('api_key', ""));
            self.var_api_secret.set(cfg.get('api_secret', ""));
            self.var_quote.set(cfg.get('quote', "USDT"));
            self.var_interval.set(cfg.get('interval', Client.KLINE_INTERVAL_5MINUTE))
            self.var_db_path.set(cfg.get('db_path', DB_PATH_DEFAULT))

            self.var_strategy_choice.set(cfg.get('strategy_choice', "EMARSI"))

            for base, row in list(self.symbol_rows.items()): row['frame'].destroy()
            self.symbol_rows.clear()
            for base, amt in cfg.get('symbols', {}).items(): self.add_symbol_row(base, amt)
            s = cfg.get('strategy', {})
            self.var_aggressiveness.set(s.get('aggressiveness', 0))
            self.var_ema_short.set(s.get('short', 20));
            self.var_ema_long.set(s.get('long', 50));
            self.var_rsi_period.set(s.get('rsi_period', 14))
            self.var_rsi_buy_max.set(s.get('rsi_buy_max', 55.0));
            self.var_rsi_sell_min.set(s.get('rsi_sell_min', 65.0))
            r = cfg.get('risk', {})
            self.var_sl.set(r.get('stop_loss_pct', 2.0));
            self.var_tp.set(r.get('take_profit_pct', 3.0))
            self.var_max_total_pos.set(r.get('max_total_positions', 5));
            self.var_max_pos_per_coin.set(r.get('max_pos_per_coin', 1))
            self.var_paper_start.set(r.get('starting_equity', 10000.0))
            self.log(f"[{now()}] Loaded config from {APP_CONFIG_FILE}")
        except Exception as e:
            self.log(f"[{now()}] Load config failed: {e}")


if __name__ == "__main__":
    app = App()
    app.mainloop()
