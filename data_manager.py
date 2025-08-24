# data_manager.py
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from binance import ThreadedWebsocketManager

from binance_client import BinanceClient
from database_manager import DatabaseManager


class DataManager:
    """
    Gestiona la adquisición y el mantenimiento de datos de mercado históricos y en tiempo real.
    """

    def __init__(self, binance_client: BinanceClient, symbols: list, db_manager: DatabaseManager, ui_queue):
        self.binance_client = binance_client
        self.symbols = symbols
        self.db_manager = db_manager
        self.ui_queue = ui_queue
        self.twm = None
        self.command_queue = None
        self.live_candle_counts = {} # NUEVO: Para contar velas en vivo

        # --- CORRECCIÓN: Separar intervalos descargables de los que son solo en vivo ---
        self.historical_intervals = ['1h', '15m', '5m', '1m']
        self.streaming_only_intervals = ['5s', '1s']
        self.all_intervals = self.historical_intervals + self.streaming_only_intervals

    def populate_initial_data(self, trading_mode: str = None):
        """
        Asegura que la base de datos tenga un historial completo de los últimos 30 días
        para los intervalos HISTÓRICOS VÁLIDOS.
        """
        if not self.binance_client:
            logging.error("Cliente de Binance no inicializado. Abortando descarga.")
            self.ui_queue.put({'type': 'sync_complete'})
            return

        logging.info("Iniciando verificación y sincronización de datos históricos de 30 días...")
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)

        # --- CORRECCIÓN: Iterar solo sobre los intervalos que se pueden descargar ---
        total_tasks = len(self.symbols) * len(self.historical_intervals)
        completed_tasks = 0

        for symbol in self.symbols:
            for interval in self.historical_intervals:
                self.ui_queue.put({'type': 'progress', 'data': {'text': f"Verificando {symbol} {interval}..."}})

                # Cargar datos existentes para encontrar huecos
                existing_data = self.db_manager.load_data(symbol, interval, start_date=start_date, end_date=end_date)

                if existing_data.empty:
                    logging.info(f"Historial vacío para {symbol} {interval}. Descarga inicial...")
                    klines = self.binance_client.get_historical_klines(symbol, interval,
                                                                       start_str=start_date.strftime("%d %b, %Y"))
                    if klines:
                        self.db_manager.save_dataframe(self._format_klines(klines), symbol, interval)

                # Actualizar datos desde la última vela guardada hasta ahora
                last_ts = self.db_manager.get_last_timestamp(symbol, interval)
                if last_ts and last_ts < end_date:
                    start_str = (last_ts + timedelta(milliseconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                    klines_new = self.binance_client.get_historical_klines(symbol, interval, start_str=start_str)
                    if klines_new:
                        self.db_manager.save_dataframe(self._format_klines(klines_new), symbol, interval)

                completed_tasks += 1
                progress = (completed_tasks / total_tasks) * 100
                self.ui_queue.put(
                    {'type': 'progress', 'data': {'value': progress, 'text': f"Sincronizado {symbol} {interval}"}})

        logging.info("Sincronización de datos históricos completada. Los datos de '1s' y '5s' se poblarán en vivo.")
        self.ui_queue.put({'type': 'progress', 'data': {'value': 100, 'text': "Sincronización completa."}})
        self.ui_queue.put({'type': 'sync_complete'})

    def populate_backtesting_data(self, symbol: str):
        """
        Descarga un conjunto de datos específico para el backtesting.
        - 30 días de historial para 1h, 15m, 5m, 1m.
        - 1 día de historial para 1s (si la API lo permite en el futuro, por ahora se omite).
        """
        self.ui_queue.put(
            {'type': 'backtest_log', 'data': f"Iniciando descarga de datos para backtest de {symbol}...\n"})
        intervals_30d = ['1h', '15m', '5m', '1m']

        end_date = datetime.now(timezone.utc)
        start_date_30d = end_date - timedelta(days=30)
        self.ui_queue.put({'type': 'backtest_log',
                           'data': f"Descargando datos de 30 días ({start_date_30d.strftime('%Y-%m-%d')} a hoy)...\n"})
        for interval in intervals_30d:
            self.ui_queue.put({'type': 'backtest_log', 'data': f" -> Obteniendo velas de {interval}...\n"})
            klines = self.binance_client.get_historical_klines(symbol, interval,
                                                               start_str=start_date_30d.strftime("%d %b, %Y"))
            if klines:
                df = self._format_klines(klines)
                self.db_manager.save_dataframe(df, symbol, interval)

        self.ui_queue.put({'type': 'backtest_log',
                           'data': "NOTA: La descarga de datos históricos de '1s' no es soportada por la API de Binance y será omitida.\n"})
        self.ui_queue.put({'type': 'backtest_log', 'data': "Descarga de datos para backtesting completada.\n"})

    def _format_klines(self, klines: list) -> pd.DataFrame:
        df = pd.DataFrame(klines, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'close_time',
                                           'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                           'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df[['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric, errors='coerce')
        return df

    def _process_message(self, msg):
        try:
            actual_msg = msg.get('data', msg)
            if actual_msg.get('e') == 'error':
                logging.error(f"Error de WebSocket: {actual_msg.get('m')}")
                return

            if actual_msg and actual_msg.get('e') == 'kline':
                kline = actual_msg['k']
                symbol, interval, current_price = kline['s'], kline['i'], float(kline['c'])

                self.ui_queue.put({'type': 'new_price', 'data': {'symbol': symbol, 'price': current_price}})
                if self.command_queue:
                    self.command_queue.put({'type': 'price_update', 'data': {symbol: current_price}})

                # --- LÓGICA DE GUARDADO EN VIVO ---
                # Guardar siempre los ticks de alta frecuencia, pero solo contar/notificar en el cierre.
                if kline['x'] or interval in self.streaming_only_intervals:
                    bar_data = {'timestamp': pd.to_datetime(kline['t'], unit='ms'), 'Open': float(kline['o']),
                                'High': float(kline['h']), 'Low': float(kline['l']), 'Close': float(kline['c']),
                                'Volume': float(kline['v'])}
                    self.db_manager.save_dataframe(pd.DataFrame([bar_data]), symbol, interval)

                # --- LÓGICA DE EVENTOS EN CIERRE DE VELA ---
                if kline['x']:
                    # Contar solo las velas de alta frecuencia cerradas
                    if interval in self.streaming_only_intervals:
                        key = f"{symbol}_{interval}"
                        self.live_candle_counts[key] = self.live_candle_counts.get(key, 0) + 1

                    # Enviar evento de vela cerrada para todos los intervalos
                    if self.command_queue:
                        self.command_queue.put(
                            {'type': 'candle_closed', 'data': {'symbol': symbol, 'interval': interval}})
        except Exception as e:
            logging.error(f"Error procesando mensaje de WebSocket: {e} - Mensaje: {msg}", exc_info=True)

    def get_live_candle_count(self, symbol: str, interval: str) -> int:
        """Devuelve el número de velas de alta frecuencia recibidas en vivo."""
        return self.live_candle_counts.get(f"{symbol}_{interval}", 0)

    def start_streaming(self, command_queue, trading_mode: str):
        self.command_queue = command_queue
        self.live_candle_counts = {}  # Resetear el contador
        logging.info("Iniciando el gestor de WebSockets...")

        # Suscribirse a TODOS los intervalos para tener los datos disponibles para cualquier modo
        streams_to_sub = [f"{s.lower()}@kline_{i}" for s in self.symbols for i in self.all_intervals]

        self.twm = ThreadedWebsocketManager(api_key=self.binance_client.api_key,
                                            api_secret=self.binance_client.api_secret,
                                            testnet=self.binance_client.use_testnet)
        self.twm.start()
        logging.info(f"Suscribiéndose a {len(streams_to_sub)} streams de WebSocket...")
        self.twm.start_multiplex_socket(callback=self._process_message, streams=streams_to_sub)

    def stop_streaming(self):
        if self.twm:
            logging.info("Deteniendo el gestor de WebSockets...")
            self.twm.stop()
            self.twm.join()