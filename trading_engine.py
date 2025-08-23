# trading_engine.py
import time
import logging
import queue
import json
from datetime import datetime

from strategy import MediumTermConfluenceStrategy, HFVWAPStrategy
from market_state import MarketState


class TradingEngine:
    def __init__(self, config, db_manager, command_queue, ui_queue, binance_client, aggressiveness: int,
                 trading_mode: str):
        self.config = config
        self.db_manager = db_manager
        self.command_queue = command_queue
        self.ui_queue = ui_queue
        self.binance_client = binance_client
        self.aggressiveness = aggressiveness
        self.trading_mode = trading_mode

        self.portfolio_config = self._load_portfolio_config()
        self.active_coins = list(self.portfolio_config.keys())
        self.risk_params = self._load_risk_params()
        self.position_sizing_params = self._load_position_sizing_params()

        self.strategy, self.strategy_intervals = self._load_strategy_config()

        self.portfolio_state_path = self.config.get('settings', 'portfolio_state_path')
        self.portfolio = self._load_portfolio_state()
        self.market_states = {symbol: MarketState(symbol) for symbol in self.active_coins}
        self.bot_states = {symbol: 'IDLE' for symbol in self.active_coins}
        self.live_prices = {}
        self.is_running = False

    def _load_portfolio_config(self):
        return {symbol.upper(): float(amount) for symbol, amount in self.config.items('portfolio')}

    def _load_position_sizing_params(self):
        return {'max_open_positions': self.config.getint('position_sizing', 'max_open_positions', fallback=5)}

    def _load_risk_params(self):
        return {'stop_loss_pct': self.config.getfloat('risk', 'stop_loss_percentage'),
                'take_profit_pct': self.config.getfloat('risk', 'take_profit_percentage')}

    def _load_strategy_config(self):
        if self.trading_mode == 'Scalping':
            strategy = HFVWAPStrategy(self.config, self.aggressiveness)
            intervals = ['15m', '1m', '5s']
        else:
            strategy = MediumTermConfluenceStrategy(self.config, self.aggressiveness)
            intervals = ['1h', '15m', '5m']
        return strategy, intervals

    def _load_portfolio_state(self):
        try:
            with open(self.portfolio_state_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {'cash': 0.0, 'positions': {}}

    def _save_portfolio_state(self):
        try:
            with open(self.portfolio_state_path, 'w') as f:
                json.dump(self.portfolio, f, indent=4)
        except Exception as e:
            logging.error(f"No se pudo guardar estado: {e}")

    def run(self):
        self.is_running = True
        logging.info(f"Motor de trading iniciando en modo '{self.trading_mode}'...")
        self.ui_queue.put({'type': 'status', 'data': f"En vivo ({self.trading_mode})..."})

        self._initial_state_load()

        # --- NUEVA LÓGICA DE HEARTBEAT ---
        heartbeat_counter = 0

        while self.is_running:
            try:
                message = self.command_queue.get(timeout=1)

                # Si llega un mensaje, reseteamos el contador del heartbeat
                heartbeat_counter = 0

                if message['type'] == 'stop':
                    self.is_running = False;
                    break
                elif message['type'] == 'candle_closed':
                    self.process_candle_closure(message['data']['symbol'], message['data']['interval'])
                elif message['type'] == 'price_update':
                    self.live_prices.update(message['data'])
                    self.run_reflex_logic()

            except queue.Empty:
                # --- LÓGICA DE HEARTBEAT ---
                # Si no hay mensajes por 1 segundo, incrementamos el contador
                heartbeat_counter += 1
                # Cada 30 segundos, enviamos un mensaje de "estoy vivo"
                if heartbeat_counter % 30 == 0:
                    log_msg = "Motor en espera, escuchando eventos de mercado..."
                    logging.info(log_msg)
                    self.ui_queue.put({'type': 'log', 'data': log_msg})
                continue

        self.shutdown()

    def _initial_state_load(self):
        logging.info("Cargando estado inicial del mercado para todas las monedas...")
        for symbol in self.active_coins:
            for interval in self.strategy_intervals:
                df = self.db_manager.load_data(symbol, interval)
                self.market_states[symbol].update_data(interval, df)
            self.market_states[symbol].calculate_all_indicators(self.config)
        logging.info("Carga inicial del estado del mercado completada.")

    def process_candle_closure(self, symbol, interval):
        if interval not in self.strategy_intervals:
            return

        logging.info(f"Vela de {interval} cerrada para {symbol}. Re-evaluando estrategia.")

        df = self.db_manager.load_data(symbol, interval)
        self.market_states[symbol].update_data(interval, df)
        self.market_states[symbol].calculate_all_indicators(self.config)

        if self.bot_states[symbol] != 'IN_POSITION':
            open_positions = len([s for s in self.bot_states.values() if s == 'IN_POSITION'])
            if open_positions >= self.position_sizing_params['max_open_positions']:
                logging.info("Máximo de posiciones abiertas alcanzado.")
                return

            self.run_deliberation_logic(symbol)

    def run_reflex_logic(self):
        if not self.portfolio['positions']: return
        for symbol, position in list(self.portfolio['positions'].items()):
            current_price = self.live_prices.get(symbol)
            if not current_price: continue
            sl_price, tp_price = position.get('stop_loss_price'), position.get('take_profit_price')
            exit_reason = None
            if sl_price and current_price <= sl_price:
                exit_reason = f"Stop-Loss alcanzado a {sl_price:.4f}"
            elif tp_price and current_price >= tp_price:
                exit_reason = f"Take-Profit alcanzado a {tp_price:.4f}"
            if exit_reason:
                pnl = (current_price - position['entry_price']) * position['size']
                logging.info(f"CERRANDO POSICIÓN para {symbol}: {exit_reason}")
                self._close_position(symbol, current_price, pnl, exit_reason)

    def run_deliberation_logic(self, symbol: str):
        log_prefix = f"[{symbol}]"
        self.ui_queue.put({'type': 'log', 'data': f"{log_prefix} Analizando tras cierre de vela..."})

        signal = self.strategy.next(self.market_states[symbol])
        reason = self.strategy.get_analysis_reason()

        self.ui_queue.put({'type': 'log', 'data': f"{log_prefix} SEÑAL GENERADA: {signal}."})
        self.ui_queue.put({'type': 'log', 'data': f"{log_prefix} Razón: {reason}"})

        trade_amount = self.portfolio_config.get(symbol.upper())
        if signal == 'BUY' and trade_amount and trade_amount > 0:
            current_price = self.live_prices.get(symbol)
            if current_price:
                self.ui_queue.put(
                    {'type': 'log', 'data': f"{log_prefix} CONCLUSIÓN: La señal es COMPRAR. ABRIENDO POSICIÓN."})
                self._open_position(symbol, current_price, trade_amount)
                self.bot_states[symbol] = 'IN_POSITION'
            else:
                self.ui_queue.put({'type': 'log',
                                   'data': f"{log_prefix} CONCLUSIÓN: Señal de COMPRA, pero sin precio en vivo para ejecutar."})
        else:
            self.ui_queue.put({'type': 'log', 'data': f"{log_prefix} CONCLUSIÓN: Sin acción de compra."})

    def _open_position(self, symbol, entry_price, trade_amount):
        if self.portfolio['cash'] < trade_amount:
            logging.warning(
                f"Fondos insuficientes para {symbol}. Se requieren ${trade_amount}, disponibles ${self.portfolio['cash']:.2f}")
            self.ui_queue.put({'type': 'log', 'data': f"ALERTA: Fondos insuficientes para abrir posición en {symbol}."})
            return

        size = trade_amount / entry_price
        sl_price = entry_price * (1 - self.risk_params['stop_loss_pct'] / 100)
        tp_price = entry_price * (1 + self.risk_params['take_profit_pct'] / 100)
        self.portfolio['positions'][symbol] = {'size': size, 'entry_price': entry_price, 'stop_loss_price': sl_price,
                                               'take_profit_price': tp_price,
                                               'timestamp': datetime.utcnow().isoformat()}
        self.portfolio['cash'] -= trade_amount
        self._save_portfolio_state()

        log_msg = (
            f"TRADE: ABRIR {size:.6f} {symbol} @ {entry_price:.4f} (SL: {sl_price:.4f}, TP: {tp_price:.4f}). Cash restante: {self.portfolio['cash']:.2f}")
        logging.info(log_msg)
        self.ui_queue.put({'type': 'log', 'data': log_msg})

    def _close_position(self, symbol, close_price, pnl, reason):
        position = self.portfolio['positions'].pop(symbol)
        self.portfolio['cash'] += position['size'] * close_price
        self._save_portfolio_state()
        self.bot_states[symbol] = 'IDLE'

        log_msg = (
            f"TRADE: CERRAR {position['size']:.6f} {symbol} @ {close_price:.4f}. Razón: {reason}. P/L: ${pnl:,.2f}. Cash total: {self.portfolio['cash']:.2f}")
        logging.info(log_msg)
        self.ui_queue.put({'type': 'log', 'data': log_msg})

    def update_portfolio_pnl_to_ui(self):
        # Esta función podría ser llamada periódicamente si es necesario, pero el reflex logic es más eficiente
        pass

    def shutdown(self):
        logging.info("El motor de trading se ha apagado.")
        self.ui_queue.put({'type': 'status', 'data': 'Motor detenido.'})