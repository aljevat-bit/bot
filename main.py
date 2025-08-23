# main.py
import os
from dotenv import load_dotenv
import configparser
from tkinter import messagebox
import logging

# Configurar logging para una salida limpia y útil (INFO para producción)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from gui import TradingApp
from database_manager import DatabaseManager


def main():
    """Punto de entrada principal de la aplicación."""
    logging.info("--- INICIANDO APLICACIÓN ---")
    load_dotenv()
    logging.info("1. Variables de entorno cargadas.")
    config = configparser.ConfigParser()
    config.read('config.ini')
    logging.info("2. Archivo config.ini leído.")

    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    use_testnet = config.getboolean('settings', 'use_testnet')
    if use_testnet:
        logging.info("3. Usando configuración de Testnet.")
        api_key = os.getenv('BINANCE_TESTNET_API_KEY')
        api_secret = os.getenv('BINANCE_TESTNET_API_SECRET')
    else:
        logging.info("3. Usando configuración de Producción (Live).")

    if not api_key or not api_secret or 'YOUR_API_KEY' in api_key:
        logging.error("Claves API no encontradas o no configuradas en .env. CERRANDO.")
        messagebox.showerror(
            "Error de Configuración",
            "Las claves API de Binance no están configuradas.\n\n"
            "Por favor, crea un archivo '.env' a partir de '.env.example' "
            "y añade tus credenciales."
        )
        return

    logging.info("4. Claves API validadas correctamente.")
    db_manager = DatabaseManager(config.get('settings', 'db_path'))
    logging.info("5. Gestor de base de datos inicializado.")

    logging.info("6. Creando la instancia de la aplicación GUI (TradingApp)...")
    app = TradingApp(config=config, db_manager=db_manager, api_key=api_key, api_secret=api_secret)
    logging.info("7. Instancia de la GUI creada. Lanzando mainloop...")
    app.mainloop()
    logging.info("--- APLICACIÓN CERRADA ---")
    db_manager.close()


if __name__ == "__main__":
    main()