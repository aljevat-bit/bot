# backtesting_engine.py
import pandas as pd
import logging
from strategy import MediumTermConfluenceStrategy, HFVWAPStrategy


from market_state import MarketState

class BacktestingEngine:
    """Motor para simular estrategias de trading en datos históricos."""

    def __init__(self, config, db_manager, symbol: str, strategy_class, balance: float, trade_amount: float,
                 aggressiveness: int = 5):
        self.config = config
        self.db_manager = db_manager
        self.symbol = symbol
        self.strategy_class = strategy_class
        self.initial_balance = balance
        self.cash = balance
        self.trade_amount = trade_amount
        self.aggressiveness = aggressiveness
        self.position = {}
        self.trades = []
        self.strategy_intervals = self._get_strategy_intervals()

    def _get_strategy_intervals(self):
        if self.strategy_class == MediumTermConfluenceStrategy:
            params = self.config['strategy_medium_term']
            return [params['context_tf'], params['setup_tf'], params['trigger_tf']]
        elif self.strategy_class == HFVWAPStrategy:
            params = self.config['strategy_scalping']
            return [params['bias_tf'], params['setup_tf'], params['trigger_tf']]
        return []

    def run(self):
        logging.info(f"Iniciando backtest con {self.strategy_class.__name__} en {self.symbol}...")

        # Cargar todos los datos necesarios
        all_data = {
            interval: self.db_manager.load_data(self.symbol, interval)
            for interval in self.strategy_intervals
        }

        # El timeframe principal para la iteración será el más corto de la estrategia
        main_tf = min(self.strategy_intervals, key=lambda x: pd.to_timedelta(x.replace('s', 'S').replace('m', 'T').replace('h', 'H')))
        main_df = all_data[main_tf]

        if main_df.empty:
            logging.warning(f"No hay datos para el timeframe principal {main_tf}, no se puede ejecutar el backtest.")
            return {"message": f"No hay datos para el timeframe principal {main_tf}."}

        # Instancia de la estrategia para usar en el bucle
        strategy_instance = self.strategy_class(self.config, self.aggressiveness)

        for i in range(1, len(main_df)):
            current_timestamp = main_df.index[i]
            current_price = main_df['Close'].iloc[i]

            # 1. Gestionar posición abierta (Stop Loss / Take Profit)
            if self.position:
                entry_price = self.position['entry_price']
                sl_price = entry_price * (1 - self.config.getfloat('risk', 'stop_loss_percentage') / 100)
                tp_price = entry_price * (1 + self.config.getfloat('risk', 'take_profit_percentage') / 100)

                if current_price <= sl_price or current_price >= tp_price:
                    self._close_position(current_timestamp, current_price)

            # 2. Buscar nuevas entradas
            if not self.position:
                market_state = MarketState(self.symbol)
                for interval, df in all_data.items():
                    # Filtrar datos hasta el momento actual
                    df_slice = df[df.index < current_timestamp]
                    if not df_slice.empty:
                        market_state.update_data(interval, df_slice)

                if not market_state.dataframes: continue

                market_state.calculate_all_indicators(self.config)

                signal, _, _ = strategy_instance.next(market_state)

                if signal == 'BUY':
                    self._open_position(current_timestamp, current_price)

        if self.position:
            self._close_position(main_df.index[-1], main_df['Close'].iloc[-1])

        return self._generate_report()

    def _open_position(self, timestamp, price):
        if self.cash >= self.trade_amount:
            size = self.trade_amount / price
            self.position = {'entry_price': price, 'size': size, 'entry_time': timestamp}
            self.cash -= self.trade_amount

    def _close_position(self, timestamp, price):
        pnl = (price - self.position['entry_price']) * self.position['size']
        self.cash += self.trade_amount + pnl

        self.trades.append({
            'entry_time': self.position['entry_time'],
            'exit_time': timestamp,
            'entry_price': self.position['entry_price'],
            'exit_price': price,
            'pnl': pnl,
            'balance': self.cash
        })
        self.position = {}

    def _generate_report(self):
        """Calcula las métricas de rendimiento y genera un informe."""
        if not self.trades:
            return {"message": "No se realizaron operaciones."}

        df_trades = pd.DataFrame(self.trades)
        successful_trades = df_trades[df_trades['pnl'] > 0].shape[0]
        total_trades = df_trades.shape[0]
        win_rate = (successful_trades / total_trades) * 100 if total_trades > 0 else 0

        total_pnl = df_trades['pnl'].sum()
        roi_percentage = (total_pnl / self.initial_balance) * 100

        # Puntuación personalizada: (Retorno %) * (Tasa de acierto %)
        # Pondera tanto la rentabilidad como la consistencia
        custom_score = (roi_percentage * win_rate) / 100 if win_rate > 0 else 0

        report = {
            "Estrategia": self.strategy_class.__name__,
            "Operaciones Totales": total_trades,
            "Operaciones Exitosas": successful_trades,
            "Tasa de Acierto (%)": f"{win_rate:.2f}%",
            "Monto Inicial (USDT)": f"${self.initial_balance:,.2f}",
            "Ganancia/Pérdida Neta (USDT)": f"${total_pnl:,.2f}",
            "Rentabilidad (%)": f"{roi_percentage:.2f}%",
            "Puntuación Personalizada": f"{custom_score:.2f} / 100"
        }
        return report