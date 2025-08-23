# market_state.py
import pandas as pd
from technical_analysis import TechnicalAnalysis


class MarketState:
    """
    Representa el 'Market Snapshot', una vista coherente del estado del mercado
    para un símbolo específico a través de múltiples marcos de tiempo.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.dataframes = {}  # Almacena los DataFrames por intervalo (ej: '1h', '15m')
        self.indicators = {}  # Almacena los indicadores calculados por intervalo

    def update_data(self, interval: str, df: pd.DataFrame):
        """Actualiza el DataFrame para un intervalo de tiempo específico."""
        self.dataframes[interval] = df

    def calculate_all_indicators(self, config):
        """
        Calcula todos los indicadores necesarios para todas las estrategias
        en todos los marcos de tiempo disponibles.
        """
        self.indicators = {}  # Reiniciar indicadores
        for interval, df in self.dataframes.items():
            if df.empty:
                continue

            self.indicators[interval] = {}
            # --- Indicadores para Mediano Plazo ---
            if 'strategy_medium_term' in config:
                mt_params = config['strategy_medium_term']
                self.indicators[interval]['EMA_SLOW'] = TechnicalAnalysis.ema(df['Close'],
                                                                              int(mt_params['ema_slow_period']))
                self.indicators[interval]['EMA_FAST'] = TechnicalAnalysis.ema(df['Close'],
                                                                              int(mt_params['ema_fast_period']))
                self.indicators[interval]['EMA_PULLBACK'] = TechnicalAnalysis.ema(df['Close'],
                                                                                  int(mt_params['ema_pullback_period']))
                self.indicators[interval]['RSI'] = TechnicalAnalysis.rsi(df['Close'], int(mt_params['rsi_period']))

            # --- Indicadores para Scalping ---
            if 'strategy_scalping' in config:
                sc_params = config['strategy_scalping']
                # VWAP (simplificado como una EMA ponderada por volumen)
                vwap = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
                self.indicators[interval]['VWAP'] = vwap
                self.indicators[interval]['VWAP_BIAS_EMA'] = TechnicalAnalysis.ema(df['Close'], int(
                    sc_params['vwap_bias_ema_period']))