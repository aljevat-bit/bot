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

try:
    from binance.client import Client
    from binance.enums import *
    from binance.exceptions import BinanceAPIException
    from binance import ThreadedWebsocketManager
except ImportError:
    print("Dependencies not found. Please run: pip install python-binance python-dotenv pandas")
    sys.exit(1)

# ---------------- UI ----------------
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

###############################################################################
# Constants & Small Utils
###############################################################################

APP_CONFIG_FILE = os.path.expanduser("~/.simple_crypto_bot_config_v3.json")
DB_PATH_DEFAULT = os.path.expanduser("~/.simple_crypto_bot_trades_v3.sqlite")

INTERVAL_MINUTES = {
    Client.KLINE_INTERVAL_1MINUTE: 1,
    Client.KLINE_INTERVAL_3MINUTE: 3,
    Client.KLINE_INTERVAL_5MINUTE: 5,
    Client.KLINE_INTERVAL_15MINUTE: 15,
    Client.KLINE_INTERVAL_1HOUR: 60,
    Client.KLINE_INTERVAL_4HOUR: 240,
}


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def round_step(value, step):
    if not step or step == 0:
        return float(value)
    from decimal import Decimal as D
    value_d = D(str(value))
    step_d = D(str(step))
    return float((value_d // step_d) * step_d)


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
# SQLite Persistence
###############################################################################

class TradeDB:
    def __init__(self, path, log_fn=print):
        self.path = path
        self.log = log_fn
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.lock = threading.Lock()
        self._create()

    def _create(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, mode TEXT, symbol TEXT,
                    side TEXT, qty REAL, price REAL, quote_amount REAL, reason TEXT
                );
            """)
            self.conn.execute("CREATE TABLE IF NOT EXISTS equity (ts TEXT PRIMARY KEY, equity REAL);")
            self.conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);")

    def set_meta(self, k, v):
        with self.lock, self.conn:
            self.conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES (?,?)", (k, str(v)))

    def get_meta(self, k, default=None):
        with self.lock:
            cur = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,))
            row = cur.fetchone()
            return row[0] if row else default

    def record_trade(self, ts, mode, symbol, side, qty, price, quote_amount, reason):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO trades (ts, mode, symbol, side, qty, price, quote_amount, reason) VALUES (?,?,?,?,?,?,?,?)",
                (ts, mode, symbol, side, safe_float(qty), safe_float(price), safe_float(quote_amount), reason)
            )

    def record_equity(self, ts, equity):
        with self.lock, self.conn:
            self.conn.execute("INSERT OR REPLACE INTO equity (ts, equity) VALUES (?,?)", (ts, safe_float(equity)))

    def fetch_equity_series(self, limit=5000):
        with self.lock:
            cur = self.conn.execute("SELECT ts, equity FROM equity ORDER BY ts ASC LIMIT ?", (limit,))
            rows = cur.fetchall()
        if not rows:
            return pd.Series(dtype=float)
        times = pd.to_datetime([r[0] for r in rows])
        vals = [safe_float(r[1]) for r in rows]
        return pd.Series(vals, index=times, dtype=float)

    def compute_analytics(self, timeframe_str):
        s = self.fetch_equity_series(limit=50000)
        if s.empty or s.size < 5:
            return {"sharpe": None, "max_drawdown": None, "peak": None}
        rets = s.pct_change().dropna()
        if rets.std() == 0 or math.isnan(rets.std()):
            sharpe = None
        else:
            minutes = INTERVAL_MINUTES.get(timeframe_str, 5)
            periods_per_year = int((60 / minutes) * 24 * 365)
            sharpe = (rets.mean() / rets.std()) * math.sqrt(periods_per_year) if rets.std() != 0 else 0
        running_max = s.cummax()
        drawdown = s / running_max - 1.0
        max_dd = float(drawdown.min()) if not drawdown.empty else None
        return {"sharpe": sharpe, "max_drawdown": max_dd, "peak": s.max()}


###############################################################################
# Market Data & Binance Wrapper
###############################################################################

class BinanceWrapper:
    def __init__(self, api_key, api_secret, use_testnet=True, log_fn=print):
        self.log = log_fn
        self.client = Client(api_key, api_secret, testnet=use_testnet)
        if use_testnet: self.client.API_URL = 'https://testnet.binance.vision/api'
        self.exchange_info = self._retry(lambda: self.client.get_exchange_info())
        self.filters = self._extract_filters(self.exchange_info)

    def _retry(self, fn, *args, **kwargs):
        for attempt in range(6):
            try:
                return fn(*args, **kwargs)
            except BinanceAPIException as e:
                rate_limit = (e.code in [-1003, -1015]) or ("Too many requests" in str(e))
                if rate_limit and attempt < 5:
                    delay = min(8.0, 0.5 * (2 ** attempt))
                    self.log(f"[{now()}] Rate limit. Backing off {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                raise
            except (URLError, HTTPError, TimeoutError) as e:
                if attempt < 5:
                    delay = min(8.0, 0.5 * (2 ** attempt))
                    self.log(f"[{now()}] Network error. Backing off {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                raise

    def _extract_filters(self, info):
        return {s['symbol']: {f['filterType']: f for f in s['filters']} for s in info['symbols']}

    def get_symbol_filters(self, symbol):
        f = self.filters.get(symbol, {})
        return {
            'lot_step': safe_float(f.get('LOT_SIZE', {}).get('stepSize')),
            'tick_size': safe_float(f.get('PRICE_FILTER', {}).get('tickSize')),
            'min_notional': safe_float(f.get('NOTIONAL', {}).get('minNotional', 10.0)),
        }

    def get_klines_df(self, symbol, interval=Client.KLINE_INTERVAL_5MINUTE, limit=250):
        raw = self._retry(self.client.get_klines, symbol=symbol, interval=interval, limit=limit)
        cols = ['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'close_time', 'qa_vol', 'trades', 'tb_base_vol',
                'tb_quote_vol', 'ignore']
        df = pd.DataFrame(raw, columns=cols, dtype=float)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        df.set_index('ts', inplace=True)
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]

    def get_price(self, symbol):
        return safe_float(self._retry(self.client.get_symbol_ticker, symbol=symbol)['price'])

    def market_buy_by_quote(self, symbol, quote_amount):
        f = self.get_symbol_filters(symbol)
        if quote_amount < f['min_notional']:
            raise ValueError(f"Amount {quote_amount} < minNotional {f['min_notional']}")
        return self._retry(self.client.create_order,
                           symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET,
                           quoteOrderQty=f"{quote_amount:.8f}")

    def market_sell_base_qty(self, symbol, base_qty):
        f = self.get_symbol_filters(symbol)
        qty = round_step(base_qty, f['lot_step'])
        if qty * self.get_price(symbol) < f['min_notional']:
            raise ValueError(f"Final quantity {qty} is below minNotional.")
        return self._retry(self.client.create_order,
                           symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET,
                           quantity=f"{qty:.8f}")


###############################################################################
# Indicators & Strategy
###############################################################################

def ema(s: pd.Series, span: int) -> pd.Series: return s.ewm(span=span, adjust=False).mean()


def rsi(s: pd.Series, period: int = 14) -> pd.Series:
    delta = s.diff()
    gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
    avg_gain, avg_loss = gain.ewm(alpha=1 / period, adjust=False).mean(), loss.ewm(alpha=1 / period,
                                                                                   adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-12)
    return 100 - (100 / (1 + rs))


class EMARSI_Strategy:
    def __init__(self, short=20, long=50, rsi_period=14, rsi_buy_max=55, rsi_sell_min=65):
        self.short, self.long = short, long
        self.rsi_period, self.rsi_buy_max, self.rsi_sell_min = rsi_period, rsi_buy_max, rsi_sell_min

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out['EMA_S'], out['EMA_L'] = ema(out['Close'], self.short), ema(out['Close'], self.long)
        out['RSI'] = rsi(out['Close'], self.rsi_period)
        return out

    def get_signal_and_rank(self, df: pd.DataFrame):
        if len(df) < max(self.long, self.short) + 5:
            return 'HOLD', 0.0, {}  # Signal, Rank, Details

        # Current candle is -1, last closed is -2, previous is -3
        s1, l1, r1, c1 = df['EMA_S'].iloc[-2], df['EMA_L'].iloc[-2], df['RSI'].iloc[-2], df['Close'].iloc[-2]
        s2, l2 = df['EMA_S'].iloc[-3], df['EMA_L'].iloc[-3]

        details = {'ema_s': s1, 'ema_l': l1, 'rsi': r1}

        crossed_up = (s2 <= l2) and (s1 > l1)
        if crossed_up and r1 <= self.rsi_buy_max:
            ema_strength = (s1 - l1) / c1
            rsi_headroom = self.rsi_sell_min - r1
            rank = ema_strength * 100 + rsi_headroom  # Simple ranking
            return 'BUY', rank, details

        crossed_down = (s2 >= l2) and (s1 < l1)
        if crossed_down or r1 >= self.rsi_sell_min:
            return 'SELL', 0.0, details

        return 'HOLD', 0.0, details


###############################################################################
# Paper Broker
###############################################################################

class PaperBroker:
    def __init__(self, price_feed: 'LivePriceFeed', db: TradeDB, starting_balance=10000.0, log_fn=print):
        self.price_feed, self.db, self.log = price_feed, db, log_fn
        self.cash = safe_float(self.db.get_meta("paper_cash", starting_balance))
        if self.cash == 0: self.cash = starting_balance
        self.db.set_meta("paper_cash", self.cash)

    def get_symbol_filters(self, symbol):
        return {'lot_step': None, 'tick_size': None, 'min_notional': 10.0}

    def get_price(self, symbol):
        p = self.price_feed.get_price(symbol)
        if p is None: raise ValueError(f"No live price for {symbol}")
        return p

    def market_buy_by_quote(self, symbol, quote_amount):
        if quote_amount < 10.0: raise ValueError("Amount < 10.0 minNotional")
        price = self.get_price(symbol)
        base_qty = quote_amount / price
        cost = base_qty * price
        if cost > self.cash: raise ValueError(f"Insufficient paper cash: need {cost:.2f}, have {self.cash:.2f}")
        self.cash -= cost
        self.db.set_meta("paper_cash", self.cash)
        self.db.record_trade(now(), "paper", symbol, "BUY", base_qty, price, cost, "paper_buy")
        return {"fills": [{"qty": str(base_qty), "price": str(price)}]}

    def market_sell_base_qty(self, symbol, base_qty):
        price = self.get_price(symbol)
        proceeds = base_qty * price
        self.cash += proceeds
        self.db.set_meta("paper_cash", self.cash)
        self.db.record_trade(now(), "paper", symbol, "SELL", base_qty, price, proceeds, "paper_sell")
        return {"fills": [{"qty": str(base_qty), "price": str(price)}]}


###############################################################################
# Fast Live Price Feed (Binance WebSocket)
###############################################################################

class LivePriceFeed(threading.Thread):
    def __init__(self, symbols, use_testnet=False, log_fn=print):
        super().__init__(daemon=True)
        self.symbols = list(symbols)
        self.use_testnet = use_testnet
        self.log = log_fn
        self._prices = {}
        self._lock = threading.Lock()
        self._running = threading.Event()
        self._twm = None

    def get_price(self, symbol):
        with self._lock: return self._prices.get(symbol)

    def set_symbols(self, symbols):
        self.symbols = list(symbols)

    def run(self):
        self._running.set()
        self._twm = ThreadedWebsocketManager(testnet=self.use_testnet)
        self._twm.start()
        streams = [s.lower() + '@miniTicker' for s in self.symbols]
        self._twm.start_multiplex_socket(callback=self._ws_cb, streams=streams)
        self.log(f"[{now()}] WebSocket price feed started for {len(self.symbols)} symbols.")
        self._running.wait()
        if self._twm: self._twm.stop()
        self.log(f"[{now()}] WebSocket price feed stopped.")

    def _ws_cb(self, msg):
        try:
            if msg and 'data' in msg:
                sym = msg['data'].get('s')
                p = safe_float(msg['data'].get('c'))
                if sym and p:
                    with self._lock: self._prices[sym] = p
        except Exception:
            pass

    def stop(self):
        self._running.clear()


###############################################################################
# Bot Engine
###############################################################################

class BotEngine(threading.Thread):
    def __init__(self, data_client, order_client, price_feed: LivePriceFeed, mode,
                 symbols_cfg, strategy_cfg, risk_cfg, timeframe, db: TradeDB,
                 ui_logger, ui_updater):
        super().__init__(daemon=True)
        self.log, self.ui_updater = ui_logger, ui_updater
        self.data, self.order, self.price_feed = data_client, order_client, price_feed
        self.mode, self.symbols_cfg, self.timeframe, self.db = mode, symbols_cfg, timeframe, db
        self.running = threading.Event()

        self.strategy = EMARSI_Strategy(**strategy_cfg)
        self.sl_pct = safe_float(risk_cfg.get('stop_loss_pct', 2.0)) / 100.0
        self.tp_pct = safe_float(risk_cfg.get('take_profit_pct', 3.0)) / 100.0
        self.min_pos = int(risk_cfg.get('min_positions', 3))
        self.max_pos = int(risk_cfg.get('max_positions', 5))
        self.starting_equity = safe_float(risk_cfg.get('starting_equity', 10000.0))

        self.positions = {}  # { 'symbol': {'qty': float, 'entry': float, 'sl': float, 'tp': float} }
        self.next_fetch_ts = {sym: 0 for sym in self.symbols_cfg.keys()}
        self.last_candle_ts = {sym: None for sym in self.symbols_cfg.keys()}
        self._last_scan_ts = 0

    def stop(self):
        self.running.clear()

    def _update_ui(self, equity, peak, drawdown):
        if not self.ui_updater: return
        pos_data = []
        for sym, pos in self.positions.items():
            price = self.price_feed.get_price(sym) or pos['entry']
            pnl_pct = (price - pos['entry']) / pos['entry'] * 100.0 if pos['entry'] else 0.0
            pos_data.append({
                'symbol': sym, 'qty': pos['qty'], 'entry': pos['entry'],
                'sl': pos['sl'], 'tp': pos['tp'], 'last_price': price,
                'pnl_pct': pnl_pct, 'value': pos['qty'] * price
            })
        self.ui_updater({'positions': pos_data, 'equity': equity, 'peak': peak, 'drawdown': drawdown})

    def _eval_equity(self):
        cash = 0
        if isinstance(self.order, PaperBroker): cash = self.order.cash
        asset_val = sum(
            pos['qty'] * (self.price_feed.get_price(sym) or pos['entry']) for sym, pos in self.positions.items())
        return cash + asset_val

    def _market_exit(self, symbol, reason=""):
        pos = self.positions.pop(symbol, None)
        if not pos: return
        try:
            self.order.market_sell_base_qty(symbol, pos['qty'])
            self.log(f"[{now()}] SELL {symbol}: {pos['qty']:.6f} @ market | reason={reason}")
        except Exception as e:
            self.log(f"[{now()}] SELL FAILED for {symbol}: {e}")
            self.positions[symbol] = pos  # Put it back if sell failed

    def run(self):
        self.running.set()
        self.log(f"[{now()}] Bot started. Mode={self.mode}, TF={self.timeframe}")
        equity = self.starting_equity
        peak_equity = equity
        last_equity_log_ts = 0

        while self.running.is_set():
            now_ts = time.time()
            try:
                # 1. Fast loop: Risk checks and UI updates
                for sym in list(self.positions.keys()):
                    pos = self.positions[sym]
                    price = self.price_feed.get_price(sym)
                    if price:
                        if price <= pos['sl']:
                            self._market_exit(sym, "stop_loss")
                        elif price >= pos['tp']:
                            self._market_exit(sym, "take_profit")

                if now_ts - last_equity_log_ts > 1.0:
                    equity = self._eval_equity()
                    peak_equity = max(peak_equity, equity)
                    drawdown = (1 - equity / peak_equity) if peak_equity > 0 else 0
                    self._update_ui(equity, peak_equity, drawdown)
                    self.db.record_equity(datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S.%f"), equity)
                    last_equity_log_ts = now_ts

                # 2. Slower loop: check for signals on new candles
                for sym in self.symbols_cfg.keys():
                    if now_ts < self.next_fetch_ts[sym]: continue
                    try:
                        df = self.data.get_klines_df(sym, self.timeframe, 250)
                        candle_ts = df.index[-2]
                        if self.last_candle_ts.get(sym) != candle_ts:
                            self.last_candle_ts[sym] = candle_ts
                            df_ind = self.strategy.compute(df)
                            signal, _, _ = self.strategy.get_signal_and_rank(df_ind)
                            if sym in self.positions and signal == 'SELL':
                                self._market_exit(sym, "signal")
                        mins = INTERVAL_MINUTES.get(self.timeframe, 5)
                        self.next_fetch_ts[sym] = (candle_ts + timedelta(minutes=mins)).timestamp()
                    except Exception as e:
                        self.log(f"[{now()}] Kline error for {sym}: {e}")
                        self.next_fetch_ts[sym] = now_ts + 30  # Retry in 30s on error

                # 3. Positional logic: Scan to fill portfolio if needed
                if len(self.positions) < self.min_pos and now_ts - self._last_scan_ts > 30:
                    self._scan_and_fill()
                    self._last_scan_ts = now_ts

            except Exception as e:
                self.log(f"[{now()}] FATAL LOOP ERROR: {e}\n{traceback.format_exc()}")
            time.sleep(0.2)

        self.log(f"[{now()}] Bot stopped.")

    def _scan_and_fill(self):
        needed = self.min_pos - len(self.positions)
        if needed <= 0 or len(self.positions) >= self.max_pos:
            return

        open_pos_symbols = set(self.positions.keys())
        potential_symbols = [s for s in self.symbols_cfg.keys() if s not in open_pos_symbols]
        self.log(f"--- Scanning for {needed} position(s). Analyzing {len(potential_symbols)} symbols. ---")

        candidates = []
        for sym in potential_symbols:
            try:
                df = self.data.get_klines_df(sym, self.timeframe, 250)
                df_ind = self.strategy.compute(df)
                signal, rank, details = self.strategy.get_signal_and_rank(df_ind)

                # Detailed Logging
                ema_s, ema_l, rsi = details.get('ema_s', 0), details.get('ema_l', 0), details.get('rsi', 0)
                s2, l2 = df_ind['EMA_S'].iloc[-3], df_ind['EMA_L'].iloc[-3]
                is_crossed = (s2 <= l2) and (ema_s > ema_l)
                is_rsi_ok = rsi <= self.strategy.rsi_buy_max

                if signal == 'BUY':
                    self.log(
                        f"  ✅ [{sym}] PASS. Qualified BUY signal. Rank: {rank:.2f} (EMA Cross: {is_crossed}, RSI: {rsi:.2f} <= {self.strategy.rsi_buy_max})")
                    candidates.append({'symbol': sym, 'rank': rank})
                else:
                    reason = ""
                    if not is_crossed:
                        reason = f"No fresh EMA crossover (Short: {ema_s:.2f}, Long: {ema_l:.2f})"
                    elif not is_rsi_ok:
                        reason = f"RSI too high ({rsi:.2f} > {self.strategy.rsi_buy_max})"
                    else:
                        reason = "In HOLD state"
                    self.log(f"  ❌ [{sym}] FAIL. {reason}")

            except Exception as e:
                self.log(f"  ⚠️ [{sym}] ERROR analyzing symbol: {e}")
                continue

        self.log("--- Scan Complete ---")

        if not candidates:
            return

        candidates.sort(key=lambda x: x['rank'], reverse=True)
        to_open = candidates[:needed]
        for entry in to_open:
            if len(self.positions) >= self.max_pos: break
            self._market_enter(entry['symbol'])

    def _market_enter(self, symbol):
        amount = safe_float(self.symbols_cfg[symbol]['amount'])
        try:
            self.log(f"[{now()}] Attempting to BUY {symbol} with amount ~{amount}")
            order = self.order.market_buy_by_quote(symbol, amount)
            fills = order.get('fills', [])
            if not fills and 'cummulativeQuoteQty' in order:  # From live API
                cprice = safe_float(order['cummulativeQuoteQty']) / safe_float(order['executedQty'])
                qty = safe_float(order['executedQty'])
            elif fills:  # From paper broker
                tot_q = sum(safe_float(f['qty']) for f in fills)
                tot_v = sum(safe_float(f['qty']) * safe_float(f['price']) for f in fills)
                cprice, qty = (tot_v / tot_q) if tot_q else (0, 0)
            else:
                raise ValueError("Order response invalid")

            if qty > 0:
                sl, tp = cprice * (1.0 - self.sl_pct), cprice * (1.0 + self.tp_pct)
                self.positions[symbol] = {'qty': qty, 'entry': cprice, 'sl': sl, 'tp': tp}
                self.log(f"[{now()}] BUY SUCCESSFUL {symbol}: {qty:.6f} @ {cprice:.4f} | SL {sl:.4f} / TP {tp:.4f}")
            else:
                self.log(f"[{now()}] BUY FAILED for {symbol}: Zero quantity in order response.")
        except Exception as e:
            self.log(f"[{now()}] BUY FAILED for {symbol}: {e}")


###############################################################################
# UI
###############################################################################

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Simple Crypto Bot (EMA+RSI) v3")
        self.geometry("1450x850")
        self.minsize(1200, 700)
        load_dotenv()

        self.symbol_rows = {}
        self.ui_queue = queue.Queue()
        self.bot_thread, self.price_feed = None, None
        self.db = TradeDB(DB_PATH_DEFAULT, log_fn=self.log)

        # --- TK Variables ---
        self.var_mode = tk.StringVar(value="paper")
        self.var_api_key = tk.StringVar(value=os.getenv("BINANCE_API_KEY", ""))
        self.var_api_secret = tk.StringVar(value=os.getenv("BINANCE_API_SECRET", ""))
        self.var_quote = tk.StringVar(value="USDT")
        self.var_interval = tk.StringVar(value=Client.KLINE_INTERVAL_5MINUTE)
        self.var_ema_short, self.var_ema_long = tk.IntVar(value=20), tk.IntVar(value=50)
        self.var_rsi_period = tk.IntVar(value=14)
        self.var_rsi_buy_max, self.var_rsi_sell_min = tk.DoubleVar(value=55.0), tk.DoubleVar(value=65.0)
        self.var_sl, self.var_tp = tk.DoubleVar(value=2.0), tk.DoubleVar(value=3.0)
        self.var_min_pos, self.var_max_pos = tk.IntVar(value=2), tk.IntVar(value=5)
        self.var_paper_start = tk.DoubleVar(value=10000.0)
        self.var_status = tk.StringVar(value="Idle")
        self.var_equity = tk.StringVar(value="Equity: 10,000.00")
        self.var_peak_equity = tk.StringVar(value="Peak: 10,000.00")
        self.var_drawdown = tk.StringVar(value="Drawdown: 0.00%")

        # --- Build UI ---
        self._build_ui()
        self._load_config()
        self.after(100, self._drain_ui_queue)
        self.after(1000, self._refresh_symbol_prices)
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _build_ui(self):
        main_pane = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left_frame = ttk.Frame(main_pane, width=450)
        right_frame = ttk.Frame(main_pane, width=800)
        main_pane.add(left_frame, weight=1)
        main_pane.add(right_frame, weight=3)
        self._build_left_pane(left_frame)
        self._build_right_pane(right_frame)

    def _build_left_pane(self, parent):
        self._build_equity_bar(parent)
        self._build_top(parent)
        self._build_symbols(parent)
        self._build_strategy(parent)
        self._build_risk(parent)
        self._build_controls(parent)

    def _build_right_pane(self, parent):
        self._build_positions_table(parent)
        self._build_log(parent)

    def _build_equity_bar(self, p):
        frm = ttk.LabelFrame(p, text="Live Performance")
        frm.pack(fill="x", padx=10, pady=5)
        ttk.Label(frm, textvariable=self.var_equity).pack(side="left", padx=10, pady=5)
        ttk.Label(frm, textvariable=self.var_peak_equity).pack(side="left", padx=10, pady=5)
        ttk.Label(frm, textvariable=self.var_drawdown).pack(side="left", padx=10, pady=5)

    def _build_top(self, p):
        frm = ttk.LabelFrame(p, text="Mode & Credentials")
        frm.pack(fill="x", padx=10, pady=5)
        f_mode = ttk.Frame(frm);
        f_mode.pack(fill='x', pady=2)
        ttk.Label(f_mode, text="Mode:").pack(side="left", padx=5)
        modes = [("Paper", "paper"), ("Testnet", "testnet"), ("Live", "live")]
        for txt, val in modes: ttk.Radiobutton(f_mode, text=txt, variable=self.var_mode, value=val).pack(side="left")
        f_keys = ttk.Frame(frm);
        f_keys.pack(fill='x', pady=2)
        ttk.Label(f_keys, text="API Key").pack(side="left", padx=5)
        ttk.Entry(f_keys, textvariable=self.var_api_key, width=40).pack(side="left")
        f_sec = ttk.Frame(frm);
        f_sec.pack(fill='x', pady=2)
        ttk.Label(f_sec, text="API Secret").pack(side="left", padx=5)
        ttk.Entry(f_sec, textvariable=self.var_api_secret, show="•", width=40).pack(side="left")
        f_data = ttk.Frame(frm);
        f_data.pack(fill='x', pady=2)
        ttk.Label(f_data, text="Quote:").pack(side="left", padx=5)
        ttk.OptionMenu(f_data, self.var_quote, "USDT", "USDT", "USDC").pack(side="left")
        ttk.Label(f_data, text="Timeframe:").pack(side="left", padx=15)
        tfs = [v for k, v in Client.__dict__.items() if k.startswith('KLINE_INTERVAL')]
        ttk.OptionMenu(f_data, self.var_interval, self.var_interval.get(), *tfs).pack(side="left")

    def _build_symbols(self, p):
        frm = ttk.LabelFrame(p, text="Symbols, Live Price, & Trade Amount")
        frm.pack(fill="x", padx=10, pady=5)
        self.symbol_frame = frm
        f_header = ttk.Frame(frm);
        f_header.pack(fill='x', padx=5, pady=2)
        ttk.Label(f_header, text="Base").pack(side="left")
        ttk.Label(f_header, text="Amount").pack(side="right")
        ttk.Label(f_header, text="Live Price").pack(side="right", padx=20)

        self.add_symbol_row("BTC", 50.0)
        self.add_symbol_row("ETH", 50.0)

        f_new = ttk.Frame(frm);
        f_new.pack(fill='x', pady=5)
        self.new_symbol_var = tk.StringVar()
        ttk.Entry(f_new, textvariable=self.new_symbol_var, width=12).pack(side="left", padx=5)
        ttk.Button(f_new, text="Add Symbol", command=self._add_symbol_from_entry).pack(side="left")

    def add_symbol_row(self, base, amount):
        base = base.upper()
        if base in self.symbol_rows: return
        frm = ttk.Frame(self.symbol_frame);
        frm.pack(fill='x', padx=5, pady=1)

        amt_var = tk.StringVar(value=str(amount))
        price_var = tk.StringVar(value="--.--")

        lbl = ttk.Label(frm, text=base, width=8);
        lbl.pack(side="left")
        ent = ttk.Entry(frm, textvariable=amt_var, width=10);
        ent.pack(side="right")
        price_lbl = ttk.Label(frm, textvariable=price_var, width=15, anchor='e');
        price_lbl.pack(side="right", padx=10)

        def remove():
            frm.destroy()
            self.symbol_rows.pop(base, None)

        btn = ttk.Button(frm, text="X", width=3, command=remove);
        btn.pack(side="left", padx=5)

        self.symbol_rows[base] = {'amount_var': amt_var, 'price_var': price_var, 'frame': frm}

    def _add_symbol_from_entry(self):
        base = self.new_symbol_var.get().strip().upper()
        if base: self.add_symbol_row(base, 50.0)
        self.new_symbol_var.set("")

    def _refresh_symbol_prices(self):
        if self.price_feed and self.price_feed.is_alive():
            quote = self.var_quote.get()
            for base, row_data in self.symbol_rows.items():
                full_symbol = base + quote
                price = self.price_feed.get_price(full_symbol)
                if price:
                    row_data['price_var'].set(f"{price:,.4f}")
        self.after(1000, self._refresh_symbol_prices)

    def _build_strategy(self, p):
        frm = ttk.LabelFrame(p, text="Strategy (EMA+RSI)")
        frm.pack(fill="x", padx=10, pady=5)
        f1 = ttk.Frame(frm);
        f1.pack(fill='x')
        ttk.Label(f1, text="EMA Short").pack(side="left", padx=5)
        ttk.Spinbox(f1, from_=3, to=200, textvariable=self.var_ema_short, width=5).pack(side="left")
        ttk.Label(f1, text="EMA Long").pack(side="left", padx=15)
        ttk.Spinbox(f1, from_=5, to=400, textvariable=self.var_ema_long, width=5).pack(side="left")
        f2 = ttk.Frame(frm);
        f2.pack(fill='x', pady=2)
        ttk.Label(f2, text="RSI Period").pack(side="left", padx=5)
        ttk.Spinbox(f2, from_=5, to=50, textvariable=self.var_rsi_period, width=5).pack(side="left")
        ttk.Label(f2, text="RSI Buy ≤").pack(side="left", padx=15)
        ttk.Spinbox(f2, from_=10, to=70, textvariable=self.var_rsi_buy_max, width=5).pack(side="left")
        ttk.Label(f2, text="RSI Sell ≥").pack(side="left", padx=15)
        ttk.Spinbox(f2, from_=30, to=90, textvariable=self.var_rsi_sell_min, width=5).pack(side="left")

    def _build_risk(self, p):
        frm = ttk.LabelFrame(p, text="Risk & Position Controls")
        frm.pack(fill="x", padx=10, pady=5)
        f1 = ttk.Frame(frm);
        f1.pack(fill='x')
        ttk.Label(f1, text="Stop Loss %").pack(side="left", padx=5)
        ttk.Spinbox(f1, from_=0.2, to=20, increment=0.1, textvariable=self.var_sl, width=5).pack(side="left")
        ttk.Label(f1, text="Take Profit %").pack(side="left", padx=15)
        ttk.Spinbox(f1, from_=0.2, to=50, increment=0.1, textvariable=self.var_tp, width=5).pack(side="left")
        f2 = ttk.Frame(frm);
        f2.pack(fill='x', pady=2)
        ttk.Label(f2, text="Min Pos").pack(side="left", padx=5)
        ttk.Spinbox(f2, from_=1, to=10, textvariable=self.var_min_pos, width=5).pack(side="left")
        ttk.Label(f2, text="Max Pos").pack(side="left", padx=15)
        ttk.Spinbox(f2, from_=1, to=10, textvariable=self.var_max_pos, width=5).pack(side="left")
        f3 = ttk.Frame(frm);
        f3.pack(fill='x', pady=2)
        ttk.Label(f3, text="Paper Start Equity").pack(side="left", padx=5)
        ttk.Entry(f3, textvariable=self.var_paper_start, width=10).pack(side="left")

    def _build_controls(self, p):
        frm = ttk.Frame(p)
        frm.pack(fill="x", padx=10, pady=10)
        self.btn_start = ttk.Button(frm, text="Start Bot", command=self._start_bot)
        self.btn_start.pack(side="left", padx=5)
        self.btn_stop = ttk.Button(frm, text="Stop Bot", command=self._stop_bot, state="disabled")
        self.btn_stop.pack(side="left", padx=5)
        ttk.Button(frm, text="Save Config", command=self._save_config).pack(side="left", padx=5)
        ttk.Label(frm, textvariable=self.var_status).pack(side="right", padx=10)

    def _build_positions_table(self, p):
        frm = ttk.LabelFrame(p, text="Open Positions")
        frm.pack(fill="x", padx=5, pady=5)
        cols = ('symbol', 'qty', 'entry', 'value', 'pnl_pct', 'sl', 'tp', 'last_price')
        self.tree = ttk.Treeview(frm, columns=cols, show='headings', height=8)
        self.tree.pack(side="left", fill="x", expand=True)
        vsb = ttk.Scrollbar(frm, orient="vertical", command=self.tree.yview)
        vsb.pack(side='right', fill='y')
        self.tree.configure(yscrollcommand=vsb.set)
        col_map = {"symbol": ("Symbol", 80), "qty": ("Qty", 100), "entry": ("Entry", 80),
                   "value": ("Value", 80), "pnl_pct": ("PnL %", 60), "sl": ("Stop Loss", 80),
                   "tp": ("Take Profit", 80), "last_price": ("Last Price", 80)}
        for c in cols:
            self.tree.heading(c, text=col_map[c][0])
            self.tree.column(c, width=col_map[c][1], anchor='center')

    def _build_log(self, p):
        frm = ttk.LabelFrame(p, text="Log")
        frm.pack(fill="both", expand=True, padx=5, pady=5)
        self.txt = tk.Text(frm, height=10, bg="#f0f0f0", fg="black")
        self.txt.pack(side="left", fill="both", expand=True)
        vsb = ttk.Scrollbar(frm, orient="vertical", command=self.txt.yview)
        vsb.pack(side='right', fill='y')
        self.txt.configure(yscrollcommand=vsb.set)

    def log(self, msg):
        self.ui_queue.put({'type': 'log', 'data': str(msg)})

    def _ui_updater_callback(self, data):
        self.ui_queue.put({'type': 'update', 'data': data})

    def _drain_ui_queue(self):
        try:
            while True:
                item = self.ui_queue.get_nowait()
                if item['type'] == 'log':
                    self.txt.insert("end", item['data'] + "\n");
                    self.txt.see("end")
                elif item['type'] == 'update':
                    d = item['data']
                    self.var_equity.set(f"Equity: {human_money(d['equity'])}")
                    self.var_peak_equity.set(f"Peak: {human_money(d['peak'])}")
                    self.var_drawdown.set(f"Drawdown: {d['drawdown']:.2%}")
                    self.tree.delete(*self.tree.get_children())
                    for p in sorted(d['positions'], key=lambda x: x['symbol']):
                        pnl_str = f"{p['pnl_pct']:.2f}%"
                        self.tree.insert("", "end", values=(
                            p['symbol'], f"{p['qty']:.6f}", f"{p['entry']:.4f}",
                            human_money(p['value']), pnl_str, f"{p['sl']:.4f}",
                            f"{p['tp']:.4f}", f"{p['last_price']:.4f}"
                        ))
        except queue.Empty:
            pass
        finally:
            self.after(100, self._drain_ui_queue)

    def _get_configs(self):
        quote = self.var_quote.get().upper()
        symbols_cfg = {
            base.upper() + quote: {'amount': row['amount_var'].get()}
            for base, row in self.symbol_rows.items()
        }
        strategy_cfg = {
            'short': self.var_ema_short.get(), 'long': self.var_ema_long.get(),
            'rsi_period': self.var_rsi_period.get(), 'rsi_buy_max': self.var_rsi_buy_max.get(),
            'rsi_sell_min': self.var_rsi_sell_min.get()
        }
        risk_cfg = {
            'stop_loss_pct': self.var_sl.get(), 'take_profit_pct': self.var_tp.get(),
            'min_positions': self.var_min_pos.get(), 'max_positions': self.var_max_pos.get(),
            'starting_equity': self.var_paper_start.get()
        }
        return symbols_cfg, strategy_cfg, risk_cfg

    def _start_bot(self):
        mode = self.var_mode.get()
        symbols_cfg, strategy_cfg, risk_cfg = self._get_configs()
        if not symbols_cfg:
            messagebox.showerror("Error", "Add at least one symbol.");
            return

        all_symbols = list(symbols_cfg.keys())
        self.price_feed = LivePriceFeed(all_symbols, use_testnet=(mode == "testnet"), log_fn=self.log)
        self.price_feed.start()
        self.log(f"[{now()}] Waiting for price feed to establish connection...")

        # Robust connection wait
        connection_wait_seconds = 10
        connection_established = False
        for i in range(connection_wait_seconds):
            if any(self.price_feed.get_price(s) for s in all_symbols):
                self.log(f"[{now()}] Price feed is live after {i + 1} second(s).")
                connection_established = True
                break
            time.sleep(1)

        if not connection_established:
            self.log(
                f"[{now()}] ERROR: Price feed failed to get data after {connection_wait_seconds} seconds. Check connection/symbols.")
            if self.price_feed: self.price_feed.stop()
            return

        try:
            use_testnet = (mode == "testnet")
            data_client = BinanceWrapper(None, None, use_testnet, log_fn=self.log)
            if mode == "paper":
                order_client = PaperBroker(self.price_feed, self.db, risk_cfg['starting_equity'], self.log)
            else:
                key, sec = self.var_api_key.get().strip(), self.var_api_secret.get().strip()
                if not key or not sec:
                    messagebox.showerror("Keys required", f"Enter API key/secret for {mode} mode.");
                    return
                order_client = BinanceWrapper(key, sec, use_testnet, self.log)
        except Exception as e:
            messagebox.showerror("Connection Failed", str(e));
            return

        self.bot_thread = BotEngine(
            data_client, order_client, self.price_feed, mode, symbols_cfg,
            strategy_cfg, risk_cfg, self.var_interval.get(), self.db,
            self.log, self._ui_updater_callback
        )
        self.bot_thread.start()
        self.btn_start.config(state="disabled");
        self.btn_stop.config(state="normal")
        self.var_status.set(f"Running ({mode})")

    def _stop_bot(self):
        if self.bot_thread: self.bot_thread.stop(); self.bot_thread.join(5)
        if self.price_feed: self.price_feed.stop(); self.price_feed.join(2)
        self.bot_thread, self.price_feed = None, None
        self.btn_start.config(state="normal");
        self.btn_stop.config(state="disabled")
        self.var_status.set("Stopped")

    def _on_closing(self):
        if self.bot_thread and self.bot_thread.is_alive():
            if messagebox.askyesno("Exit", "Bot is running. Are you sure you want to exit?"):
                self._stop_bot()
                self.destroy()
        else:
            self.destroy()

    def _save_config(self):
        symbols = {base: row['amount_var'].get() for base, row in self.symbol_rows.items()}
        s_cfg, r_cfg = self._get_configs()[1:]
        cfg = {
            'mode': self.var_mode.get(), 'api_key': self.var_api_key.get(),
            'api_secret': self.var_api_secret.get(), 'quote': self.var_quote.get(),
            'interval': self.var_interval.get(), 'symbols': symbols,
            'strategy': s_cfg, 'risk': r_cfg
        }
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
            self.var_mode.set(cfg.get('mode', "paper"))
            self.var_api_key.set(cfg.get('api_key', ""))
            self.var_api_secret.set(cfg.get('api_secret', ""))
            self.var_quote.set(cfg.get('quote', "USDT"))
            self.var_interval.set(cfg.get('interval', Client.KLINE_INTERVAL_5MINUTE))

            for base, row in list(self.symbol_rows.items()): row['frame'].destroy()
            self.symbol_rows.clear()
            for base, amt in cfg.get('symbols', {}).items(): self.add_symbol_row(base, amt)

            s = cfg.get('strategy', {})
            self.var_ema_short.set(s.get('short', 20));
            self.var_ema_long.set(s.get('long', 50))
            self.var_rsi_period.set(s.get('rsi_period', 14))
            self.var_rsi_buy_max.set(s.get('rsi_buy_max', 55.0));
            self.var_rsi_sell_min.set(s.get('rsi_sell_min', 65.0))

            r = cfg.get('risk', {})
            self.var_sl.set(r.get('stop_loss_pct', 2.0));
            self.var_tp.set(r.get('take_profit_pct', 3.0))
            self.var_min_pos.set(r.get('min_positions', 2));
            self.var_max_pos.set(r.get('max_positions', 5))
            self.var_paper_start.set(r.get('starting_equity', 10000.0))

            self.log(f"[{now()}] Loaded config from {APP_CONFIG_FILE}")
        except Exception as e:
            self.log(f"[{now()}] Load config failed: {e}")


if __name__ == "__main__":
    app = App()
    app.mainloop()
