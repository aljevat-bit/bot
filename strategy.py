# strategy.py
import pandas as pd
from market_state import MarketState
from technical_analysis import TechnicalAnalysis


class BaseStrategy:
    """Clase base para todas las estrategias."""

    def __init__(self, config, aggressiveness: int):
        self.config = config
        self.aggressiveness = aggressiveness
        self.analysis_reason = "Iniciando análisis..."

    def next(self, state: MarketState) -> tuple[str, int, str]:
        """Devuelve una tupla: (Señal, Progreso %, Razón)"""
        raise NotImplementedError

    def get_analysis_reason(self) -> str:
        return self.analysis_reason


class MediumTermConfluenceStrategy(BaseStrategy):
    """Implementa la estrategia de 'Confluencia de Momentum' y reporta su progreso."""

    def next(self, state: MarketState) -> tuple[str, int, str]:
        params = self.config['strategy_medium_term']

        # --- Fase 1: Filtro de Régimen de Mercado (Contexto en 1h) ---
        ctx_tf = params['context_tf']
        if ctx_tf not in state.indicators or state.indicators[ctx_tf]['EMA_SLOW'].empty:
            return 'HOLD', 0, f"Faltan datos de contexto en {ctx_tf}"

        ema_slow = state.indicators[ctx_tf]['EMA_SLOW'].iloc[-1]
        ema_fast = state.indicators[ctx_tf]['EMA_FAST'].iloc[-1]

        regime = None
        if ema_fast > ema_slow:
            regime = 'ALLOW_LONGS'
        elif ema_fast < ema_slow:
            regime = 'ALLOW_SHORTS'
        else:
            return 'HOLD', 0, f"Régimen lateral en {ctx_tf}"

        reason = f"Paso 1/3: Régimen '{regime}' confirmado en {ctx_tf}."
        progress = 33

        # --- Fase 2: Configuración de Entrada (Retroceso en 15m) ---
        setup_tf = params['setup_tf']
        if setup_tf not in state.dataframes or setup_tf not in state.indicators:
            return 'HOLD', progress, f"Faltan datos de configuración en {setup_tf}"

        current_price = state.dataframes[setup_tf]['Close'].iloc[-1]
        ema_pullback = state.indicators[setup_tf]['EMA_PULLBACK'].iloc[-1]

        in_buy_zone = (regime == 'ALLOW_LONGS' and current_price <= ema_pullback)
        in_sell_zone = (regime == 'ALLOW_SHORTS' and current_price >= ema_pullback)

        # Con agresividad máxima, se ignora el filtro de pullback
        if self.aggressiveness == 10 and regime == 'ALLOW_LONGS':
            in_buy_zone = True
            reason = f"Paso 2/3: En zona de retroceso (ignorado por agresividad máxima)."
        elif self.aggressiveness == 10 and regime == 'ALLOW_SHORTS':
            in_sell_zone = True
            reason = f"Paso 2/3: En zona de retroceso (ignorado por agresividad máxima)."
        elif not (in_buy_zone or in_sell_zone):
            return 'HOLD', progress, "Precio fuera de la zona de retroceso en 15m."
        else:
            reason = f"Paso 2/3: Precio en zona de retroceso en {setup_tf}."
        progress = 66

        # --- Fase 3: Disparador de Entrada (RSI en 5m) ---
        trigger_tf = params['trigger_tf']
        if trigger_tf not in state.indicators or state.indicators[trigger_tf]['RSI'].empty:
            return 'HOLD', progress, f"Faltan datos de disparo en {trigger_tf}"

        rsi = state.indicators[trigger_tf]['RSI'].iloc[-1]
        # Lógica de agresividad corregida: un valor más alto facilita el trade
        rsi_oversold = int(params['rsi_oversold']) + self.aggressiveness
        rsi_overbought = int(params['rsi_overbought']) - self.aggressiveness

        if in_buy_zone and rsi < rsi_oversold:
            reason = f"Paso 3/3: RSI sobrevendido ({rsi:.1f} < {rsi_oversold}) en {trigger_tf}."
            return 'BUY', 100, reason

        if in_sell_zone and rsi > rsi_overbought:
            reason = f"Paso 3/3: RSI sobrecomprado ({rsi:.1f} > {rsi_overbought}) en {trigger_tf}."
            return 'SELL', 100, reason

        return 'HOLD', progress, "En zona, pero RSI no confirma entrada."


class HFVWAPStrategy(BaseStrategy):
    """Implementa la estrategia 'Breakout/Retest de VWAP' y reporta su progreso."""

    def next(self, state: MarketState) -> tuple[str, int, str]:
        params = self.config['strategy_scalping']

        # --- Fase 1: Filtro de Sesgo Intradía (15m) ---
        bias_tf = params['bias_tf']
        if bias_tf not in state.indicators or state.indicators[bias_tf]['VWAP'].empty:
            return 'HOLD', 0, f"Faltan datos de sesgo en {bias_tf}"

        current_price = state.dataframes[bias_tf]['Close'].iloc[-1]
        vwap = state.indicators[bias_tf]['VWAP'].iloc[-1]
        bias_ema = state.indicators[bias_tf]['VWAP_BIAS_EMA'].iloc[-1]

        bias = None
        if current_price > vwap and current_price > bias_ema:
            bias = 'LONG'
        elif current_price < vwap and current_price < bias_ema:
            bias = 'SHORT'

        if not bias:
            return 'HOLD', 0, "Sin sesgo intradía claro (precio vs VWAP/EMA)"

        reason = f"Paso 1/2: Sesgo '{bias}' confirmado en {bias_tf}."
        progress = 50

        # --- Fase 2: Disparo de Momentum (5s) ---
        trigger_tf = params['trigger_tf']
        if trigger_tf not in state.dataframes or len(state.dataframes[trigger_tf]) < 5:
            return 'HOLD', progress, f"Faltan datos de disparo en {trigger_tf}"

        recent_candles = state.dataframes[trigger_tf].tail(int(params['momentum_ticks']))

        if bias == 'LONG':
            is_momentum_up = all(recent_candles['Close'].diff().dropna() > 0)
            if is_momentum_up:
                reason = f"Paso 2/2: {params['momentum_ticks']} velas de 5s consecutivas al alza."
                return 'BUY', 100, reason

        if bias == 'SHORT':
            is_momentum_down = all(recent_candles['Close'].diff().dropna() < 0)
            if is_momentum_down:
                reason = f"Paso 2/2: {params['momentum_ticks']} velas de 5s consecutivas a la baja."
                return 'SELL', 100, reason

        return 'HOLD', progress, f"Sesgo {bias} detectado, pero sin confirmación de momentum."