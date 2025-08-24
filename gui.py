# gui.py
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import json
from datetime import datetime
import logging

from binance_client import BinanceClient
from data_manager import DataManager
from database_manager import DatabaseManager
from trading_engine import TradingEngine
from backtesting_engine import BacktestingEngine
from strategy import MediumTermConfluenceStrategy, HFVWAPStrategy


class TradingApp(tk.Tk):
    def __init__(self, config, db_manager, api_key, api_secret, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("Bot de Trading Modular v2.2")
        self.geometry("1400x900")

        self.config = config
        self.db_manager = db_manager
        self.api_key = api_key
        self.api_secret = api_secret
        self.command_queue, self.ui_queue = queue.Queue(), queue.Queue()
        self.backend_thread = self.trading_engine = self.data_manager = self.binance_client = None
        self.current_portfolio_state = None
        self.use_testnet = self.config.getboolean('settings', 'use_testnet')
        self.aggressiveness = tk.IntVar(value=5)
        self.trading_mode = tk.StringVar(value="Mediano Plazo")

        self._setup_ui()
        self.after(100, self.process_ui_queue)
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.load_settings_to_ui()

    def _setup_ui(self):
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        control_frame = ttk.LabelFrame(main_frame, text="Controles Principales", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        self.connect_button = ttk.Button(control_frame, text="1. Conectar", command=self.connect_to_binance)
        self.connect_button.pack(side=tk.LEFT, padx=5)
        self.sync_button = ttk.Button(control_frame, text="2. Sincronizar Datos", command=self.start_historical_sync,
                                      state=tk.DISABLED)
        self.sync_button.pack(side=tk.LEFT, padx=5)
        self.start_button = ttk.Button(control_frame, text="3. Iniciar Bot", command=self.start_bot, state=tk.DISABLED)
        self.start_button.pack(side=tk.LEFT, padx=5)
        self.stop_button = ttk.Button(control_frame, text="4. Detener Bot", command=self.stop_bot, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        mode_frame = ttk.Frame(control_frame)
        mode_frame.pack(side=tk.LEFT, padx=20)
        ttk.Label(mode_frame, text="Modo de Trading:").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="Mediano Plazo", variable=self.trading_mode, value="Mediano Plazo").pack(
            side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="Scalping", variable=self.trading_mode, value="Scalping").pack(side=tk.LEFT)

        self.progress_label = ttk.Label(control_frame, text="", font=("Helvetica", 9))
        self.progress_label.pack(side=tk.LEFT, padx=10)
        self.progress_bar = ttk.Progressbar(control_frame, length=150)
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)

        aggressiveness_frame = ttk.Frame(control_frame)
        aggressiveness_frame.pack(side=tk.RIGHT, padx=20)
        ttk.Label(aggressiveness_frame, text="Agresividad:").pack(side=tk.LEFT)
        self.aggressiveness_slider = ttk.Scale(
            aggressiveness_frame, from_=1, to=10, orient=tk.HORIZONTAL, variable=self.aggressiveness,
            command=lambda s: self.aggressiveness_label.config(text=f"{int(float(s))}")
        )
        self.aggressiveness_slider.pack(side=tk.LEFT, padx=5)
        self.aggressiveness_label = ttk.Label(aggressiveness_frame, text=f"{self.aggressiveness.get()}", width=2)
        self.aggressiveness_label.pack(side=tk.LEFT)

        testnet_status = "ACTIVADA" if self.use_testnet else "DESACTIVADA"
        testnet_color = "dark orange" if self.use_testnet else "gray"
        ttk.Label(control_frame, text=f"Testnet: {testnet_status}", foreground=testnet_color,
                  font=("Helvetica", 10, "italic")).pack(side=tk.RIGHT, padx=10)

        self.status_label = ttk.Label(control_frame, text="Estado: Inactivo", font=("Helvetica", 10, "bold"))
        self.status_label.pack(side=tk.RIGHT, padx=10)

        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=10)
        self._create_dashboard_tab()
        self._create_settings_tab()
        self._create_backtesting_tab()

    def _create_dashboard_tab(self):
        dashboard_tab = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(dashboard_tab, text="Dashboard")
        top_frame = ttk.Frame(dashboard_tab)
        top_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        left_column = ttk.Frame(top_frame)
        left_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        log_frame = ttk.LabelFrame(left_column, text="Log de Operaciones y Análisis", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        positions_frame = ttk.LabelFrame(left_column, text="Posiciones Abiertas (Bot)", padding="10")
        positions_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        pos_cols = ('Símbolo', 'Tamaño', 'P. Entrada', 'P. Actual', 'P/L ($)', 'Stop Loss', 'Take Profit')
        self.positions_tree = ttk.Treeview(positions_frame, columns=pos_cols, show='headings')
        for col in pos_cols: self.positions_tree.heading(col, text=col); self.positions_tree.column(col, width=100,
                                                                                                    anchor=tk.CENTER)
        self.positions_tree.pack(fill=tk.BOTH, expand=True)

        right_column = ttk.Frame(top_frame, width=380)
        right_column.pack(side=tk.RIGHT, fill=tk.Y)
        right_column.pack_propagate(False)

        portfolio_frame = ttk.LabelFrame(right_column, text="Estado del Portafolio (Bot)", padding="10")
        portfolio_frame.pack(fill=tk.X)
        self.cash_label = ttk.Label(portfolio_frame, text="Efectivo (Bot): $0.00", font=("Helvetica", 10))
        self.cash_label.pack(anchor=tk.W)
        self.positions_value_label = ttk.Label(portfolio_frame, text="Valor Posiciones: $0.00", font=("Helvetica", 10))
        self.positions_value_label.pack(anchor=tk.W)
        self.total_value_label = ttk.Label(portfolio_frame, text="Valor Total (Bot): $0.00",
                                           font=("Helvetica", 12, "bold"))
        self.total_value_label.pack(anchor=tk.W, pady=(5, 0))

        real_portfolio_frame = ttk.LabelFrame(right_column, text="Portafolio Real Binance (USDT)", padding="10")
        real_portfolio_frame.pack(fill=tk.X, pady=10)
        rp_cols = ('Activo', 'Cantidad', 'Valor (USDT)')
        self.real_portfolio_tree = ttk.Treeview(real_portfolio_frame, columns=rp_cols, show='headings', height=5)
        for col in rp_cols: self.real_portfolio_tree.heading(col, text=col)
        self.real_portfolio_tree.pack(fill=tk.X)

        prices_frame = ttk.LabelFrame(right_column, text="Precios en Vivo", padding="10")
        prices_frame.pack(fill=tk.X, pady=10)
        price_cols = ('Símbolo', 'Precio')
        self.prices_tree = ttk.Treeview(prices_frame, columns=price_cols, show='headings', height=4)
        for col in price_cols: self.prices_tree.heading(col, text=col)
        self.prices_tree.pack(fill=tk.X)

        # --- NUEVO: Panel de Progreso de Estrategia ---
        strategy_progress_frame = ttk.LabelFrame(right_column, text="Progreso de Estrategias", padding="10")
        strategy_progress_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        prog_cols = ('Símbolo', 'Señal', 'Progreso (%)', 'Paso Actual')
        self.strategy_progress_tree = ttk.Treeview(strategy_progress_frame, columns=prog_cols, show='headings', height=5)
        for col in prog_cols:
            self.strategy_progress_tree.heading(col, text=col)
            width = 120 if col == 'Paso Actual' else 80
            self.strategy_progress_tree.column(col, width=width, anchor=tk.W)
        self.strategy_progress_tree.pack(fill=tk.BOTH, expand=True)

        # --- RESTAURADO: Panel de Indicadores en Vivo ---
        self.indicators_frame = ttk.LabelFrame(right_column, text="Indicadores en Vivo (Estrategia Activa)",
                                               padding="10")
        self.indicators_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        # Las columnas se poblarán dinámicamente
        self.indicators_tree = ttk.Treeview(self.indicators_frame, columns=(), show='headings')
        self.indicators_tree.pack(fill=tk.BOTH, expand=True)


        # --- NUEVO: Barra de Progreso de Scalping Dedicada ---
        self.scalping_progress_frame = ttk.LabelFrame(left_column, text="Progreso de Recolección de Datos (Scalping)",
                                                      padding="10")
        # Se packea/despackea dinámicamente
        self.scalping_progress_label = ttk.Label(self.scalping_progress_frame, text="Iniciando...",
                                                 font=("Helvetica", 9))
        self.scalping_progress_label.pack(side=tk.LEFT, padx=5)
        self.scalping_progress_bar = ttk.Progressbar(self.scalping_progress_frame, length=200)
        self.scalping_progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

    def _create_settings_tab(self):
        settings_tab = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(settings_tab, text="Configuración")

        sizing_frame = ttk.LabelFrame(settings_tab, text="Dimensionamiento de Posición Global", padding="10")
        sizing_frame.pack(fill=tk.X, pady=5)
        ttk.Label(sizing_frame, text="Máximo de Posiciones Abiertas Simultáneamente:").pack(side=tk.LEFT, padx=5)
        self.max_pos_var = tk.StringVar()
        ttk.Entry(sizing_frame, textvariable=self.max_pos_var, width=10).pack(side=tk.LEFT)

        portfolio_frame = ttk.LabelFrame(settings_tab, text="Símbolos y Montos a Operar", padding="10")
        portfolio_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        add_coin_frame = ttk.Frame(portfolio_frame)
        add_coin_frame.pack(fill=tk.X, pady=5)
        ttk.Label(add_coin_frame, text="Símbolo:").pack(side=tk.LEFT, padx=5)
        self.new_coin_entry = ttk.Entry(add_coin_frame, width=15)
        self.new_coin_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(add_coin_frame, text="Monto por Trade ($):").pack(side=tk.LEFT, padx=5)
        self.new_amount_entry = ttk.Entry(add_coin_frame, width=10)
        self.new_amount_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(add_coin_frame, text="Añadir / Actualizar", command=self.add_update_coin).pack(side=tk.LEFT, padx=5)
        ttk.Button(add_coin_frame, text="Eliminar Seleccionado", command=self.remove_coin).pack(side=tk.LEFT, padx=5)

        self.portfolio_tree = ttk.Treeview(portfolio_frame, columns=('Símbolo', 'Monto Trade'), show='headings')
        self.portfolio_tree.heading('Símbolo', text='Símbolo Permitido')
        self.portfolio_tree.heading('Monto Trade', text='Monto por Trade (USDT)')
        self.portfolio_tree.pack(fill=tk.BOTH, expand=True, pady=5)

        # --- NUEVO: Paneles de Configuración de Estrategias ---
        strategy_settings_frame = ttk.Frame(settings_tab)
        strategy_settings_frame.pack(fill=tk.X, pady=10)

        # Frame para Mediano Plazo
        mt_frame = ttk.LabelFrame(strategy_settings_frame, text="Parámetros Estrategia Mediano Plazo", padding="10")
        mt_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, anchor=tk.N)

        self.mt_params = {}
        mt_param_list = [
            'context_tf', 'ema_slow_period', 'ema_fast_period', 'setup_tf', 'ema_pullback_period',
            'trigger_tf', 'rsi_period', 'rsi_oversold', 'rsi_overbought'
        ]
        for param in mt_param_list:
            row = ttk.Frame(mt_frame)
            row.pack(fill=tk.X, pady=2)
            ttk.Label(row, text=f"{param}:", width=20).pack(side=tk.LEFT)
            var = tk.StringVar()
            ttk.Entry(row, textvariable=var, width=15).pack(side=tk.LEFT)
            self.mt_params[param] = var

        # Frame para Scalping
        sc_frame = ttk.LabelFrame(strategy_settings_frame, text="Parámetros Estrategia Scalping", padding="10")
        sc_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, anchor=tk.N)

        self.sc_params = {}
        sc_param_list = ['bias_tf', 'vwap_bias_ema_period', 'setup_tf', 'trigger_tf', 'momentum_ticks']
        for param in sc_param_list:
            row = ttk.Frame(sc_frame)
            row.pack(fill=tk.X, pady=2)
            ttk.Label(row, text=f"{param}:", width=20).pack(side=tk.LEFT)
            var = tk.StringVar()
            ttk.Entry(row, textvariable=var, width=15).pack(side=tk.LEFT)
            self.sc_params[param] = var

        ttk.Button(settings_tab, text="Guardar Cambios en config.ini", command=self.save_settings).pack(pady=10)
        ttk.Label(settings_tab, text="Nota: El bot debe ser reiniciado para que los cambios de estrategia surtan efecto.",
                  font=("Helvetica", 9, "italic")).pack(pady=5)

    def _create_backtesting_tab(self):
        backtesting_tab = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(backtesting_tab, text="Backtesting")

        controls_frame = ttk.Frame(backtesting_tab)
        controls_frame.pack(fill=tk.X, pady=5)
        ttk.Label(controls_frame, text="Símbolo:").pack(side=tk.LEFT, padx=5)
        self.backtest_symbol_var = tk.StringVar()
        self.backtest_symbol_combo = ttk.Combobox(controls_frame, textvariable=self.backtest_symbol_var,
                                                  state="readonly")
        self.backtest_symbol_combo.pack(side=tk.LEFT, padx=5)

        self.backtest_button = ttk.Button(controls_frame, text="Iniciar Backtest", command=self.start_backtest)
        self.backtest_button.pack(side=tk.LEFT, padx=10)

        progress_frame = ttk.Frame(backtesting_tab)
        progress_frame.pack(fill=tk.X, pady=5)
        self.backtest_progress_label = ttk.Label(progress_frame, text="Listo para iniciar backtest.")
        self.backtest_progress_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

        results_frame = ttk.LabelFrame(backtesting_tab, text="Reporte de Resultados", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        self.backtest_results_text = scrolledtext.ScrolledText(results_frame, wrap=tk.WORD, state=tk.DISABLED)
        self.backtest_results_text.pack(fill=tk.BOTH, expand=True)

    def start_backtest(self):
        symbol = self.backtest_symbol_var.get()
        if not symbol:
            messagebox.showwarning("Sin Símbolo", "Por favor, selecciona un símbolo para el backtest.")
            return

        self.backtest_button.config(state=tk.DISABLED)
        self.backtest_progress_label.config(text=f"Iniciando backtest para {symbol}...")
        self.backtest_results_text.config(state=tk.NORMAL)
        self.backtest_results_text.delete('1.0', tk.END)
        self.backtest_results_text.config(state=tk.DISABLED)
        self.notebook.select(self.notebook.tabs()[-1])

        backtesting_db_path = self.config.get('settings', 'backtesting_db_path')
        threading.Thread(target=self._execute_backtest, args=(symbol, backtesting_db_path), daemon=True).start()

    def _execute_backtest(self, symbol, db_path):
        try:
            self.ui_queue.put({'type': 'backtest_log', 'data': f"--- INICIANDO BACKTEST PARA {symbol} ---\n"})

            backtest_db_manager = DatabaseManager(db_path)
            backtest_data_manager = DataManager(self.binance_client, [symbol], backtest_db_manager, self.ui_queue)
            backtest_data_manager.populate_backtesting_data(symbol)

            self.ui_queue.put(
                {'type': 'backtest_log', 'data': "\n--- SIMULANDO ESTRATEGIA DE MEDIANO PLAZO ---\n"})
            try:
                engine_mt = BacktestingEngine(self.config, backtest_db_manager, symbol, MediumTermConfluenceStrategy, 10000, 500, self.aggressiveness.get())
                report_mt = engine_mt.run()
                report_str_mt = "\n".join([f"- {key}: {value}" for key, value in report_mt.items()])
                self.ui_queue.put({'type': 'backtest_log', 'data': f"REPORTE DE MEDIANO PLAZO:\n{report_str_mt}\n"})
            except Exception as e:
                logging.error(f"Error en backtest de Mediano Plazo: {e}", exc_info=True)
                self.ui_queue.put({'type': 'backtest_log', 'data': f"ERROR en backtest de Mediano Plazo: {e}\n"})

            self.ui_queue.put(
                {'type': 'backtest_log', 'data': "\n--- SIMULANDO ESTRATEGIA DE SCALPING ---\n"})
            try:
                # Nota: El backtesting de HFVWAPStrategy en datos de 1s puede ser muy lento y consumir mucha memoria.
                # Se usa agresividad 10 y monto de 100 para el ejemplo.
                engine_sc = BacktestingEngine(self.config, backtest_db_manager, symbol, HFVWAPStrategy, 10000, 100, 10)
                report_sc = engine_sc.run()
                report_str_sc = "\n".join([f"- {key}: {value}" for key, value in report_sc.items()])
                self.ui_queue.put({'type': 'backtest_log', 'data': f"REPORTE DE SCALPING:\n{report_str_sc}\n"})
            except Exception as e:
                logging.error(f"Error en backtest de Scalping: {e}", exc_info=True)
                self.ui_queue.put({'type': 'backtest_log', 'data': f"ERROR en backtest de Scalping: {e}\n"})

            self.ui_queue.put({'type': 'backtest_log', 'data': "\n--- BACKTEST COMPLETADO ---\n"})
            backtest_db_manager.close()
        except Exception as e:
            logging.error(f"Ocurrió un error durante el backtest: {e}", exc_info=True)
            self.ui_queue.put({'type': 'backtest_log', 'data': f"\nERROR: Ocurrió un error durante el backtest: {e}\n"})
        finally:
            self.ui_queue.put({'type': 'backtest_finished'})

    def connect_to_binance(self):
        self.status_label.config(text="Estado: Conectando a Binance...")
        self.connect_button.config(state=tk.DISABLED)
        threading.Thread(target=self._execute_connection, daemon=True).start()

    def _execute_connection(self):
        self.binance_client = BinanceClient(self.api_key, self.api_secret, self.use_testnet)
        if self.binance_client.connect_and_prepare():
            balance_info = self.binance_client.get_account_balance()
            if balance_info:
                self.ui_queue.put({'type': 'initial_balance_update', 'data': balance_info})
                self._initialize_portfolio_state(balance_info['total_usdt'])
            self.ui_queue.put({'type': 'status', 'data': 'Conectado. Sincroniza datos históricos.'})
            active_coins = [item[0] for item in self.config.items('portfolio')]
            self.data_manager = DataManager(
                binance_client=self.binance_client, symbols=[c.upper() for c in active_coins],
                db_manager=self.db_manager, ui_queue=self.ui_queue
            )
            self.ui_queue.put({'type': 'connection_success'})
        else:
            self.ui_queue.put({'type': 'status', 'data': 'Error de conexión. Revisa claves/red.'})
            self.ui_queue.put({'type': 'connection_failed'})

    def _initialize_portfolio_state(self, initial_cash: float):
        path = self.config.get('settings', 'portfolio_state_path')
        try:
            with open(path, 'x') as f:
                initial_state = {'cash': initial_cash, 'positions': {}}
                json.dump(initial_state, f, indent=4)
                logging.info(f"Archivo de portafolio creado con capital inicial de ${initial_cash:,.2f}")
        except FileExistsError:
            logging.info("Archivo de portafolio ya existe. No se sobreescribe.")

    def start_historical_sync(self):
        self.status_label.config(text="Estado: Sincronizando datos históricos...")
        self.sync_button.config(state=tk.DISABLED)
        self.progress_bar['value'] = 0
        self.progress_label.config(text="Iniciando descarga...")
        mode = self.trading_mode.get()
        threading.Thread(target=self.data_manager.populate_initial_data, args=(mode,), daemon=True).start()

    def start_bot(self):
        self.status_label.config(text="Estado: Iniciando streaming y motor de trading...")
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.aggressiveness_slider.config(state=tk.DISABLED)
        mode = self.trading_mode.get()

        if mode == 'Scalping':
            self.scalping_progress_frame.pack(fill=tk.X, pady=5, before=self.log_text.master)
            self.scalping_progress_bar['value'] = 0
            self.scalping_progress_label.config(text="Iniciando recolección de datos...")

        self.data_manager.start_streaming(self.command_queue, mode)

        aggressiveness_value = self.aggressiveness.get()
        self.trading_engine = TradingEngine(
            config=self.config, db_manager=self.db_manager, data_manager=self.data_manager,
            command_queue=self.command_queue, ui_queue=self.ui_queue,
            binance_client=self.binance_client, aggressiveness=aggressiveness_value, trading_mode=mode
        )
        self.backend_thread = threading.Thread(target=self.trading_engine.run, daemon=True)
        self.backend_thread.start()

    def stop_bot(self):
        if self.backend_thread and self.backend_thread.is_alive():
            self.command_queue.put({'type': 'stop'})
        if self.data_manager:
            self.data_manager.stop_streaming()
        self.start_button.config(state=tk.NORMAL)
        self.sync_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.aggressiveness_slider.config(state=tk.NORMAL)
        self.status_label.config(text="Estado: Deteniendo...")

    def process_ui_queue(self):
        try:
            while True:
                message = self.ui_queue.get_nowait()
                msg_type, data = message.get('type'), message.get('data')
                if msg_type == 'log':
                    self.log_text.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {data}\n")
                    self.log_text.see(tk.END)
                elif msg_type == 'status':
                    self.status_label.config(text=f"Estado: {data}")
                elif msg_type == 'backtest_log':
                    self.backtest_results_text.config(state=tk.NORMAL)
                    self.backtest_results_text.insert(tk.END, data)
                    self.backtest_results_text.config(state=tk.DISABLED)
                    self.backtest_results_text.see(tk.END)
                elif msg_type == 'backtest_finished':
                    self.backtest_progress_label.config(text="Backtest completado.")
                    self.backtest_button.config(state=tk.NORMAL)
                elif msg_type == 'connection_success':
                    self.sync_button.config(state=tk.NORMAL)
                    messagebox.showinfo("Conexión Exitosa",
                                        "Conectado a Binance.\nPaso 2: Sincroniza los Datos Históricos.")
                elif msg_type == 'connection_failed':
                    self.connect_button.config(state=tk.NORMAL)
                elif msg_type == 'initial_balance_update':
                    self.cash_label.config(text=f"Efectivo (Bot): ${data['total_usdt']:,.2f}")
                    self.total_value_label.config(text=f"Valor Total (Bot): ${data['total_usdt']:,.2f}")
                    for i in self.real_portfolio_tree.get_children(): self.real_portfolio_tree.delete(i)
                    sorted_assets = sorted(data['assets'].items(), key=lambda item: item[1]['usdt_value'], reverse=True)
                    for asset, details in sorted_assets:
                        self.real_portfolio_tree.insert('', 'end', values=(asset, f"{details['qty']:.6f}",
                                                                           f"${details['usdt_value']:,.2f}"))
                elif msg_type == 'progress':
                    self.progress_bar['value'] = data.get('value', self.progress_bar['value'])
                    self.progress_label.config(text=data.get('text', ''))
                elif msg_type == 'scalping_progress':
                    if data.get('visible', True):
                        self.scalping_progress_bar['value'] = data.get('value', 0)
                        self.scalping_progress_label.config(text=data.get('text', ''))
                    else:
                        self.scalping_progress_frame.pack_forget()
                elif msg_type == 'sync_complete':
                    self.start_button.config(state=tk.NORMAL)
                    self.progress_label.config(text="Sincronización completa.")
                    messagebox.showinfo("Sincronización Completa",
                                        "Los datos históricos están al día.\nPaso 3: Inicia el Bot.")
                elif msg_type == 'positions_update':
                    self.update_portfolio_display(data)
                elif msg_type == 'indicator_update':
                    self.update_indicators_display(data['symbol'], data['indicators'])
                elif msg_type == 'strategy_update':
                    self.update_strategy_progress_display(data)
                elif msg_type == 'new_price':
                    self.update_live_prices_ticker(data['symbol'], data['price'])
                    self.update_portfolio_display()
        except queue.Empty:
            pass
        finally:
            self.after(100, self.process_ui_queue)

    def update_strategy_progress_display(self, update_data: dict):
        symbol = update_data['symbol']
        item_id = f"progress_{symbol}"

        signal = update_data['signal']
        progress = update_data['progress']
        reason = update_data['reason']

        values = (symbol, signal, f"{progress}%", reason)

        if self.strategy_progress_tree.exists(item_id):
            self.strategy_progress_tree.item(item_id, values=values)
        else:
            self.strategy_progress_tree.insert('', 'end', iid=item_id, values=values)

    def update_indicators_display(self, symbol: str, indicators: dict):
        """Actualiza dinámicamente el treeview de indicadores."""
        # --- Configuración dinámica de columnas ---
        current_cols = self.indicators_tree['columns']
        # Ordenar indicadores alfabéticamente para consistencia
        new_cols = ['Símbolo'] + sorted(list(indicators.keys()))

        if tuple(current_cols) != tuple(new_cols):
            self.indicators_tree['columns'] = new_cols
            for col in new_cols:
                self.indicators_tree.heading(col, text=col)
                self.indicators_tree.column(col, width=100, anchor=tk.W)

        # --- Actualización de valores ---
        item_id = f"indicator_{symbol}"

        # Formatear valores a 4 decimales si son flotantes
        values = [symbol] + [f"{indicators.get(k, 'N/A'):.4f}" if isinstance(indicators.get(k), float) else indicators.get(k, 'N/A')
                             for k in new_cols[1:]]

        if self.indicators_tree.exists(item_id):
            self.indicators_tree.item(item_id, values=values)
        else:
            self.indicators_tree.insert('', 'end', iid=item_id, values=values)

    def update_live_prices_ticker(self, symbol, price):
        item_id = f"price_{symbol}"
        values = (symbol, f"{price:,.4f}")
        if self.prices_tree.exists(item_id):
            self.prices_tree.item(item_id, values=values)
        else:
            self.prices_tree.insert('', 'end', iid=item_id, values=values)

    def update_portfolio_display(self, portfolio_data=None):
        if portfolio_data: self.current_portfolio_state = portfolio_data
        if not self.current_portfolio_state: return
        cash = self.current_portfolio_state['cash']
        positions = self.current_portfolio_state['positions']
        live_prices = self.current_portfolio_state.get('live_prices', {})
        self.cash_label.config(text=f"Efectivo (Bot): ${cash:,.2f}")
        for i in self.positions_tree.get_children(): self.positions_tree.delete(i)
        positions_value = 0
        for symbol, data in positions.items():
            current_price = live_prices.get(symbol, data['entry_price'])
            position_value = data['size'] * current_price
            positions_value += position_value
            pnl = (current_price - data['entry_price']) * data['size']
            sl_price, tp_price = data.get('stop_loss_price', 'N/A'), data.get('take_profit_price', 'N/A')
            self.positions_tree.insert('', 'end', values=(
                symbol, f"{data['size']:.6f}", f"{data['entry_price']:.4f}", f"{current_price:.4f}", f"{pnl:,.2f}",
                f"{sl_price:.4f}" if isinstance(sl_price, float) else sl_price,
                f"{tp_price:.4f}" if isinstance(tp_price, float) else tp_price
            ))
        self.positions_value_label.config(text=f"Valor Posiciones: ${positions_value:,.2f}")
        self.total_value_label.config(text=f"Valor Total (Bot): ${cash + positions_value:,.2f}")

    def load_settings_to_ui(self):
        self.max_pos_var.set(self.config.get('position_sizing', 'max_open_positions', fallback='5'))
        for i in self.portfolio_tree.get_children(): self.portfolio_tree.delete(i)
        if self.config.has_section('portfolio'):
            symbols = [symbol.upper() for symbol, _ in self.config.items('portfolio')]
            self.backtest_symbol_combo['values'] = symbols
            if symbols: self.backtest_symbol_var.set(symbols[0])
            for symbol, amount in self.config.items('portfolio'):
                self.portfolio_tree.insert('', 'end',
                                           values=(symbol.upper(), f"{float(amount):.2f}" if amount else "0.00"))

        # Cargar parámetros de estrategia
        if self.config.has_section('strategy_medium_term'):
            for param, var in self.mt_params.items():
                var.set(self.config.get('strategy_medium_term', param, fallback=''))

        if self.config.has_section('strategy_scalping'):
            for param, var in self.sc_params.items():
                var.set(self.config.get('strategy_scalping', param, fallback=''))

    def add_update_coin(self):
        symbol = self.new_coin_entry.get().strip().upper()
        amount_str = self.new_amount_entry.get().strip()
        if not symbol or not amount_str:
            messagebox.showerror("Error de Entrada", "El símbolo y el monto no pueden estar vacíos.")
            return
        try:
            amount = float(amount_str)
            if not self.config.has_section('portfolio'): self.config.add_section('portfolio')
            self.config.set('portfolio', symbol.lower(), str(amount))
            self.load_settings_to_ui()
            self.new_coin_entry.delete(0, tk.END)
            self.new_amount_entry.delete(0, tk.END)
        except ValueError:
            messagebox.showerror("Error de Entrada", "El monto debe ser un número válido.")

    def remove_coin(self):
        selected_item = self.portfolio_tree.selection()
        if not selected_item: return
        symbol_to_remove = self.portfolio_tree.item(selected_item[0])['values'][0]
        self.config.remove_option('portfolio', symbol_to_remove.lower())
        self.load_settings_to_ui()

    def save_settings(self):
        try:
            self.config.set('position_sizing', 'max_open_positions', self.max_pos_var.get())

            # Guardar portfolio
            self.config.remove_section('portfolio')
            self.config.add_section('portfolio')
            for row in self.portfolio_tree.get_children():
                symbol, amount = self.portfolio_tree.item(row)['values']
                self.config.set('portfolio', str(symbol).lower(), str(amount))

            # Guardar parámetros de estrategias
            for param, var in self.mt_params.items():
                self.config.set('strategy_medium_term', param, var.get())
            for param, var in self.sc_params.items():
                self.config.set('strategy_scalping', param, var.get())

            with open('config.ini', 'w') as configfile:
                self.config.write(configfile)
            messagebox.showinfo("Éxito", "La configuración ha sido guardada en config.ini.")
        except Exception as e:
            messagebox.showerror("Error al Guardar", f"No se pudo guardar el archivo de configuración:\n{e}")

    def on_closing(self):
        if messagebox.askokcancel("Salir", "¿Estás seguro de que quieres salir?"):
            if self.backend_thread and self.backend_thread.is_alive():
                self.stop_bot()
                self.backend_thread.join(timeout=3)
            self.destroy()