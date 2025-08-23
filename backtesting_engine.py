# backtesting_engine.py
import pandas as pd
import logging
from strategy import MediumTermConfluenceStrategy, HFVWAPStrategy


class BacktestingEngine:
    """Motor para simular estrategias de trading en datos históricos."""

    def __init__(self, config, data: pd.DataFrame, strategy_class, balance: float, trade_amount: float,
                 aggressiveness: int = 5):
        self.config = config
        self.data = data
        self.strategy_class = strategy_class
        self.initial_balance = balance
        self.cash = balance
        self.trade_amount = trade_amount
        self.aggressiveness = aggressiveness
        self.position = {}
        self.trades = []
        self.params = self._get_strategy_params()

    def _get_strategy_params(self):
        """Obtiene los parámetros correctos del config para la estrategia que se está probando."""
        if self.strategy_class == SMACrossRSIStrategy:
            return self.config['strategy_medium_term']
        elif self.strategy_class == ScalpingStrategy:
            return self.config['strategy_scalping']
        return {}

    def run(self):
        """Ejecuta la simulación vela por vela."""
        logging.info(f"Iniciando backtest con {self.strategy_class.__name__}...")

        # Preparar los datos con los indicadores de la estrategia
        strategy_instance_for_indicators = self.strategy_class(self.data.copy(), self.params, self.aggressiveness)
        self.data = strategy_instance_for_indicators.df  # El df ahora tiene las columnas de indicadores

        for i in range(1, len(self.data)):
            current_price = self.data['Close'].iloc[i]

            # 1. Gestionar posición abierta (Stop Loss / Take Profit)
            if self.position:
                entry_price = self.position['entry_price']
                sl_price = entry_price * (1 - self.config.getfloat('risk', 'stop_loss_percentage') / 100)
                tp_price = entry_price * (1 + self.config.getfloat('risk', 'take_profit_percentage') / 100)

                if current_price <= sl_price or current_price >= tp_price:
                    self._close_position(self.data.index[i], current_price)

            # 2. Buscar nuevas entradas
            if not self.position:
                # Usamos los datos hasta la vela ANTERIOR para decidir en la vela actual
                historical_slice = self.data.iloc[:i]
                strategy = self.strategy_class(historical_slice.copy(), self.params, self.aggressiveness)
                signal = strategy.next()

                if signal == 'BUY':
                    self._open_position(self.data.index[i], current_price)

        # Si queda una posición abierta al final, la cerramos con el último precio
        if self.position:
            self._close_position(self.data.index[-1], self.data['Close'].iloc[-1])

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