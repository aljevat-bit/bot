# database_manager.py
import sqlite3
import pandas as pd
import logging
import threading

class DatabaseManager:
    """
    Gestiona todas las interacciones con la base de datos SQLite de forma segura para hilos (thread-safe).
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.lock = threading.Lock()
        logging.info(f"Conexión a la base de datos establecida en: {self.db_path}")

    def save_dataframe(self, df: pd.DataFrame, symbol: str, interval: str):
        if df.empty: return
        table_name = f"{symbol.upper()}_{interval}"
        df_copy = df.copy()
        if 'timestamp' in df_copy.columns:
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            df_copy.set_index('timestamp', inplace=True)
        try:
            with self.lock:
                df_copy.to_sql(table_name, self.conn, if_exists='append', index=True)
        except Exception as e:
            logging.error(f"Error al guardar datos en la tabla '{table_name}': {e}")

    def load_data(self, symbol: str, interval: str, start_date=None, end_date=None) -> pd.DataFrame:
        table_name = f"{symbol.upper()}_{interval}"
        query = f"SELECT * FROM \"{table_name}\""
        conditions = []
        if start_date: conditions.append(f"timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'")
        if end_date: conditions.append(f"timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")
        if conditions: query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp"
        try:
            with self.lock:
                df = pd.read_sql_query(query, self.conn, index_col='timestamp', parse_dates=['timestamp'])
            if not df.index.tz: df = df.tz_localize('UTC')
            df = df[~df.index.duplicated(keep='first')]
            return df
        except Exception:
            return pd.DataFrame()

    def get_last_timestamp(self, symbol: str, interval: str) -> pd.Timestamp | None:
        table_name = f"{symbol.upper()}_{interval}"
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                if cursor.fetchone() is None: return None
                query = f"SELECT MAX(timestamp) FROM \"{table_name}\""
                result = pd.read_sql_query(query, self.conn)
            last_time = result.iloc[0, 0]
            if pd.notna(last_time):
                return pd.to_datetime(last_time).tz_localize('UTC')
            return None
        except Exception: return None

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Conexión a la base de datos cerrada.")