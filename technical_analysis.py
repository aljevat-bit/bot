# technical_analysis.py
import pandas as pd


class TechnicalAnalysis:
    """
    Una biblioteca de funciones estáticas para calcular indicadores técnicos.
    Este es un módulo de utilidad puro sin dependencias del proyecto.
    """

    @staticmethod
    def sma(series: pd.Series, length: int) -> pd.Series:
        """Calcula la Media Móvil Simple (SMA)."""
        return series.rolling(window=length).mean()

    @staticmethod
    def ema(series: pd.Series, length: int) -> pd.Series:
        """Calcula la Media Móvil Exponencial (EMA)."""
        return series.ewm(span=length, adjust=False).mean()

    @staticmethod
    def rsi(series: pd.Series, length: int = 14) -> pd.Series:
        """Calcula el Índice de Fuerza Relativa (RSI)."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1 / length, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1 / length, adjust=False).mean()

        # Evitar división por cero
        rs = gain / loss
        rs = rs.fillna(0)  # Llenar NaNs que pueden surgir si loss es 0

        rsi = 100 - (100 / (1 + rs))
        return rsi