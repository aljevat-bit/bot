# binance_client.py
import time
import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

class BinanceClient:
    """
    Una clase envolvente para el cliente de Binance que gestiona la conexión,
    la sincronización de tiempo, el manejo de errores y los límites de tasa.
    """
    def __init__(self, api_key: str, api_secret: str, use_testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.use_testnet = use_testnet
        self.client = self._create_client()
        self.exchange_info = None
        self.symbol_info = {}

    def _create_client(self) -> Client:
        client = Client(self.api_key, self.api_secret, tld='com', testnet=self.use_testnet)
        return client

    def connect_and_prepare(self) -> bool:
        try:
            logging.info("Conectando a Binance...")
            self.client.ping()
            logging.info("Ping a Binance exitoso.")
            server_time = self.client.get_server_time()
            logging.info(f"Hora del servidor de Binance: {server_time['serverTime']}")
            logging.info("Obteniendo información del exchange...")
            self.exchange_info = self.client.get_exchange_info()
            self._cache_symbol_info()
            logging.info("Información del exchange obtenida y cacheada.")
            return True
        except (BinanceAPIException, BinanceRequestException) as e:
            logging.error(f"Error al conectar con Binance: {e}")
            return False
        except Exception as e:
            logging.error(f"Ocurrió un error inesperado durante la conexión: {e}")
            return False

    def _cache_symbol_info(self):
        if not self.exchange_info: return
        for s_info in self.exchange_info['symbols']:
            self.symbol_info[s_info['symbol']] = {
                'price_filter': next((f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER'), None),
                'lot_size_filter': next((f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE'), None),
                'min_notional_filter': next((f for f in s_info['filters'] if f['filterType'] == 'MIN_NOTIONAL'), None)
            }

    def get_symbol_info(self, symbol: str) -> dict | None:
        return self.symbol_info.get(symbol)

    def execute_api_call(self, method, *args, **kwargs):
        max_retries, backoff_factor = 5, 2
        for attempt in range(max_retries):
            try:
                return method(*args, **kwargs)
            except BinanceAPIException as e:
                if e.status_code in [429, 418]:
                    time.sleep(backoff_factor ** attempt)
                else: raise e
            except BinanceRequestException as e: raise e
        logging.error("No se pudo ejecutar la llamada a la API después de múltiples reintentos.")
        return None

    def get_historical_klines(self, symbol: str, interval: str, start_str: str = None, end_str: str = None, limit: int = 1000):
        return self.execute_api_call(self.client.get_historical_klines,
                                     symbol=symbol, interval=interval,
                                     start_str=start_str, end_str=end_str, limit=limit)

    def get_account_balance(self) -> dict | None:
        logging.info("Obteniendo balance de la cuenta de Binance...")
        try:
            account_info = self.execute_api_call(self.client.get_account)
            if not account_info: return None
            balances = [b for b in account_info['balances'] if float(b['free']) > 0 or float(b['locked']) > 0]
            tickers = self.execute_api_call(self.client.get_all_tickers)
            if not tickers: return None
            prices = {t['symbol']: float(t['price']) for t in tickers}
            total_usdt_value, asset_details = 0.0, {}
            for balance in balances:
                asset, qty = balance['asset'], float(balance['free']) + float(balance['locked'])
                usdt_value = 0.0
                if asset == 'USDT': usdt_value = qty
                elif asset.startswith('LD'): continue
                else:
                    pair_usdt = f"{asset}USDT"
                    if pair_usdt in prices: usdt_value = qty * prices[pair_usdt]
                if usdt_value > 0.01:
                    total_usdt_value += usdt_value
                    asset_details[asset] = {'qty': qty, 'usdt_value': usdt_value}
            logging.info(f"Valor total del portafolio estimado: ${total_usdt_value:,.2f} USDT")
            return {'total_usdt': total_usdt_value, 'assets': asset_details}
        except Exception as e:
            logging.error(f"No se pudo obtener el balance de la cuenta: {e}")
            return None