import sys
import json
import os
import sqlite3
import html
import uuid
from datetime import datetime
from threading import Lock
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

from app.config import load_config, save_config as write_config
from app.constants import CHAT_HISTORY_PATH, MAX_MESSAGE_LENGTH
from app.validation import validate_message
from app.xmpp.service import XMPPService
from app.kafka_broker import KafkaBroker
from rest_server import RESTServer
from logger import MessageLogger

os.environ['LANG'] = 'ru_RU.UTF-8'


class XMPPGUI(QMainWindow):
    # --- Межпоточные сигналы (объявлены на уровне класса) ---
    sig_received_message = pyqtSignal(str, str, str)   # from_jid, message, msg_id
    sig_delivery_received = pyqtSignal(str)            # msg_id
    sig_receipt_received = pyqtSignal(str)             # msg_id
    sig_send_failed = pyqtSignal(str)                  # msg_id
    sig_log = pyqtSignal(str, str)                     # message, level
    sig_message_sent = pyqtSignal(str, str, str)       # to_user, message, msg_id (для REST)

    def __init__(self):
        super().__init__()
        self.config = load_config()
        retention = self.config.get("logging", {}).get("retention_days", 14)
        self.xmpp_service = None
        self.rest_server = None
        self.kafka_broker = None
        self.logger = MessageLogger(retention_days=retention)
        self.chats = {}
        self.chats_lock = Lock()
        self.current_contact = None
        self.pending_messages = {}
        self.pending_messages_lock = Lock()
        self._chat_dirty = False
        self._history_dirty = False

        # --- Подключение сигналов к слотам (выполняются в главном потоке) ---
        self.sig_received_message.connect(self._process_received_message)
        self.sig_delivery_received.connect(self._on_delivery_received_safe)
        self.sig_receipt_received.connect(self._on_receipt_received_safe)
        self.sig_send_failed.connect(self._on_send_failed_safe)
        self.sig_log.connect(self._log_callback_safe)
        self.sig_message_sent.connect(self._on_message_sent_safe)

        # Инициализация Kafka
        if self.config.get("kafka", {}).get("enabled", False):
            self._init_kafka()

        self.load_chat_history()
        self.init_ui()
        QTimer.singleShot(3000, self.auto_start)
        QTimer.singleShot(5000, self.check_for_updates)

    def persist_settings(self):
        try:
            write_config(self.config)
        except Exception as e:
            self.log_callback(f"Ошибка сохранения настроек: {str(e)}", "ERROR")

    def _init_kafka(self):
        """Инициализация Kafka брокера."""
        try:
            kafka_config = self.config.get("kafka", {})
            bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")

            self.kafka_broker = KafkaBroker(bootstrap_servers, self.log_callback)
            if self.kafka_broker.connect():
                self.log_callback("Kafka брокер инициализирован", "INFO")
            else:
                self.log_callback("Ошибка подключения к Kafka", "ERROR")
                self.kafka_broker = None
        except Exception as exc:
            self.log_callback(f"Ошибка инициализации Kafka: {exc}", "ERROR")
            self.kafka_broker = None

    def load_chat_history(self):
        """Load chat history from disk with thread-safety."""
        history_file = CHAT_HISTORY_PATH
        if os.path.exists(history_file):
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with self.chats_lock:
                    self.chats = data
            except (json.JSONDecodeError, OSError):
                with self.chats_lock:
                    self.chats = {}

    def save_chat_history(self):
        """Save chat history to disk with thread-safety."""
        history_file = CHAT_HISTORY_PATH
        with self.chats_lock:
            data = self.chats.copy()
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def init_ui(self):
        self.setWindowTitle("XMPP Мессенджер")
        self.setGeometry(100, 100, 1300, 800)

        font = QFont("Segoe UI", 9)
        self.setFont(font)

        self.setStyleSheet("""
            QMainWindow { background-color: #36393f; }
            QLabel { color: #dcddde; }
            QGroupBox { color: #dcddde; border: none; margin-top: 8px; padding-top: 8px; font-weight: bold; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; color: #8e9297; }
            QLineEdit, QTextEdit { background-color: #40444b; color: #dcddde; border: none; border-radius: 4px; padding: 6px; font-size: 9pt; }
            QLineEdit:focus, QTextEdit:focus { border: 1px solid #7289da; }
            QListWidget { background-color: #2f3136; color: #dcddde; border: none; outline: none; }
            QListWidget::item { padding: 8px 12px; border: none; }
            QListWidget::item:selected { background-color: #40444b; }
            QListWidget::item:hover { background-color: #393c41; }
            QPushButton { background-color: #7289da; color: white; border: none; padding: 6px 12px; border-radius: 4px; font-weight: bold; font-size: 9pt; }
            QPushButton:hover { background-color: #677bc4; }
            QPushButton:pressed { background-color: #5b6eae; }
            QPushButton:disabled { background-color: #4f545c; }
            QTabWidget::pane { background-color: #36393f; border: none; }
            QTabBar::tab { background-color: #2f3136; color: #8e9297; padding: 6px 16px; min-width: 100px; font-size: 10pt; border: none; }
            QTabBar::tab:selected { background-color: #36393f; color: #ffffff; border-bottom: 2px solid #7289da; }
            QTabBar::tab:hover { color: #dcddde; }
            QStatusBar { background-color: #2f3136; color: #8e9297; }
            QTextEdit#chat_display { background-color: #36393f; border: none; padding: 8px; }
            QScrollBar:vertical { background-color: #2f3136; width: 8px; border-radius: 4px; }
            QScrollBar::handle:vertical { background-color: #40444b; border-radius: 4px; min-height: 20px; }
            QScrollBar::handle:vertical:hover { background-color: #4f545c; }
            QMenu { background-color: #2f3136; color: #dcddde; border: 1px solid #40444b; }
            QMenu::item:selected { background-color: #40444b; }
            QDialog { background-color: #2f3136; }
            QTableWidget { background-color: #2f3136; color: #dcddde; gridline-color: #40444b; selection-background-color: #40444b; }
            QHeaderView::section { background-color: #202225; color: #8e9297; padding: 6px; border: none; }
            QComboBox { background-color: #40444b; color: #dcddde; border: none; border-radius: 4px; padding: 4px 8px; }
            QComboBox::drop-down { border: none; }
            QComboBox QAbstractItemView { background-color: #2f3136; color: #dcddde; selection-background-color: #40444b; }
        """)

        tabs = QTabWidget()

        chat_tab = QWidget()
        self.setup_chat_tab(chat_tab)
        tabs.addTab(chat_tab, "💬 Чаты")

        history_tab = QWidget()
        self.setup_history_tab(history_tab)
        tabs.addTab(history_tab, "📜 История")

        logs_tab = QWidget()
        self.setup_logs_tab(logs_tab)
        tabs.addTab(logs_tab, "📋 Логи")

        settings_tab = QWidget()
        self.setup_settings_tab(settings_tab)
        tabs.addTab(settings_tab, "⚙️ Настройки")

        info_tab = QWidget()
        self.setup_info_tab(info_tab)
        tabs.addTab(info_tab, "ℹ️ О программе")

        self.setCentralWidget(tabs)

        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.status_label = QLabel("🔴 Не подключено")
        self.statusBar.addWidget(self.status_label)

        # Таймер обновления чата — реже и только при изменениях
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self._tick_ui_update)
        self.update_timer.start(1000)

        if not os.path.exists("logs"):
            os.makedirs("logs")

        self.refresh_chats_list()

    def _tick_ui_update(self):
        """Периодическое обновление UI — только если есть изменения."""
        if self._chat_dirty:
            self._chat_dirty = False
            self.refresh_chat_display()
        if self._history_dirty:
            self._history_dirty = False
            self.load_history()

    def setup_history_tab(self, tab):
        layout = QVBoxLayout()
        tab.setLayout(layout)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        top_panel = QFrame()
        top_panel.setStyleSheet("background-color: #2f3136; border-bottom: 1px solid #202225;")
        top_panel.setFixedHeight(50)
        top_layout = QHBoxLayout()
        top_panel.setLayout(top_layout)
        top_layout.setContentsMargins(16, 8, 16, 8)

        title_label = QLabel("📜 ИСТОРИЯ СООБЩЕНИЙ")
        title_label.setStyleSheet("color: #ffffff; font-weight: bold; font-size: 12pt;")
        top_layout.addWidget(title_label)
        top_layout.addStretch()

        filter_label = QLabel("Фильтр:")
        filter_label.setStyleSheet("color: #8e9297;")
        top_layout.addWidget(filter_label)

        self.history_contact_combo = QComboBox()
        self.history_contact_combo.setMinimumWidth(250)
        self.history_contact_combo.setStyleSheet("""
            QComboBox { background-color: #40444b; color: #dcddde; border: none; border-radius: 4px; padding: 5px 10px; }
            QComboBox::drop-down { border: none; }
            QComboBox QAbstractItemView { background-color: #2f3136; color: #dcddde; selection-background-color: #40444b; }
        """)
        self.history_contact_combo.addItem("Все диалоги")
        self.history_contact_combo.currentTextChanged.connect(self.load_history)
        top_layout.addWidget(self.history_contact_combo)

        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setFixedSize(32, 32)
        self.refresh_btn.setToolTip("Обновить")
        self.refresh_btn.setStyleSheet("QPushButton { background-color: #40444b; border: none; border-radius: 4px; color: #dcddde; font-size: 14pt; } QPushButton:hover { background-color: #7289da; }")
        self.refresh_btn.clicked.connect(self.load_history)
        top_layout.addWidget(self.refresh_btn)

        self.clean_btn = QPushButton("🗑️")
        self.clean_btn.setFixedSize(32, 32)
        self.clean_btn.setToolTip("Очистить старые логи")
        self.clean_btn.setStyleSheet("QPushButton { background-color: #40444b; border: none; border-radius: 4px; color: #dcddde; font-size: 12pt; } QPushButton:hover { background-color: #f04747; }")
        self.clean_btn.clicked.connect(self.clean_logs_manual)
        top_layout.addWidget(self.clean_btn)

        layout.addWidget(top_panel)

        self.history_table = QTableWidget()
        self.history_table.setColumnCount(6)
        self.history_table.setHorizontalHeaderLabels(["Тип", "Отправитель", "Получатель", "Сообщение", "Время отправки", "Время доставки"])
        self.history_table.setAlternatingRowColors(True)
        self.history_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.history_table.setSelectionMode(QTableWidget.SingleSelection)
        self.history_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.history_table.setWordWrap(True)
        self.history_table.setShowGrid(False)

        header = self.history_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header.setStyleSheet("""
            QHeaderView::section {
                background-color: #2f3136;
                color: #8e9297;
                font-weight: bold;
                font-size: 9pt;
                padding: 12px 8px;
                border: none;
                border-bottom: 1px solid #202225;
            }
        """)

        self.history_table.setStyleSheet("""
            QTableWidget {
                background-color: #36393f;
                alternate-background-color: #2f3136;
                color: #dcddde;
                font-size: 9pt;
                outline: none;
                border: none;
            }
            QTableWidget::item {
                padding: 10px 8px;
                border-bottom: 1px solid #40444b;
            }
            QTableWidget::item:selected {
                background-color: #40444b;
            }
        """)

        self.history_table.verticalHeader().setDefaultSectionSize(45)
        self.history_table.verticalHeader().setVisible(False)
        self.history_table.setSortingEnabled(True)

        layout.addWidget(self.history_table)

        bottom_panel = QFrame()
        bottom_panel.setStyleSheet("background-color: #2f3136; border-top: 1px solid #202225;")
        bottom_panel.setFixedHeight(40)
        bottom_layout = QHBoxLayout()
        bottom_panel.setLayout(bottom_layout)
        bottom_layout.setContentsMargins(16, 8, 16, 8)

        self.history_status = QLabel("Готов")
        self.history_status.setStyleSheet("color: #8e9297; font-size: 9pt;")
        bottom_layout.addWidget(self.history_status)
        bottom_layout.addStretch()

        layout.addWidget(bottom_panel)

        self.history_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.history_table.customContextMenuRequested.connect(self.show_history_context_menu)

        self.load_history()

    def show_history_context_menu(self, position):
        menu = QMenu()
        copy_action = menu.addAction("📋 Копировать текст сообщения")
        copy_action.triggered.connect(self.copy_selected_history)
        menu.exec_(self.history_table.viewport().mapToGlobal(position))

    def copy_selected_history(self):
        row = self.history_table.currentRow()
        if row >= 0:
            message_item = self.history_table.item(row, 3)
            if message_item:
                QApplication.clipboard().setText(message_item.text())
                self.history_status.setText("✅ Скопировано")
                QTimer.singleShot(2000, lambda: self.history_status.setText("Готов"))

    def load_history(self):
        try:
            selected = self.history_contact_combo.currentText()

            self.history_table.setRowCount(0)
            self.history_status.setText("Загрузка...")

            db_path = "logs/messages.db"
            if not os.path.exists(db_path):
                self.history_status.setText("❌ База данных не найдена")
                return

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            if selected == "Все диалоги":
                cursor.execute('''
                    SELECT type, sender, recipient, message, send_time, delivery_time, log_time
                    FROM messages 
                    ORDER BY id DESC 
                    LIMIT 500
                ''')
            else:
                clean_contact = selected.split('/')[0]
                cursor.execute('''
                    SELECT type, sender, recipient, message, send_time, delivery_time, log_time
                    FROM messages 
                    WHERE sender LIKE ? OR recipient LIKE ?
                    ORDER BY id DESC 
                    LIMIT 500
                ''', (f'%{clean_contact}%', f'%{clean_contact}%'))

            rows = cursor.fetchall()
            conn.close()

            self.history_table.setRowCount(len(rows))

            for i, row in enumerate(rows):
                msg_type = row[0]
                sender = row[1]
                recipient = row[2]
                message = row[3][:150] + "..." if len(row[3]) > 150 else row[3]
                send_time = row[4][:16] if row[4] else "-"
                delivery_time = row[5][:16] if row[5] else "-"

                type_color = "#43b581" if msg_type == "SENT" else "#7289da"
                type_icon = "📤" if msg_type == "SENT" else "📥"

                type_item = QTableWidgetItem(f"{type_icon} {msg_type}")
                type_item.setForeground(QColor(type_color))
                sender_item = QTableWidgetItem(sender.split('/')[0] if sender else "-")
                recipient_item = QTableWidgetItem(recipient.split('/')[0] if recipient else "-")
                message_item = QTableWidgetItem(message)
                send_time_item = QTableWidgetItem(send_time)
                delivery_time_item = QTableWidgetItem(delivery_time)

                self.history_table.setItem(i, 0, type_item)
                self.history_table.setItem(i, 1, sender_item)
                self.history_table.setItem(i, 2, recipient_item)
                self.history_table.setItem(i, 3, message_item)
                self.history_table.setItem(i, 4, send_time_item)
                self.history_table.setItem(i, 5, delivery_time_item)

            self.history_status.setText(f"📊 Загружено {len(rows)} сообщений")

        except Exception as e:
            self.history_status.setText(f"❌ Ошибка: {str(e)}")

    def clean_logs_manual(self):
        reply = QMessageBox.question(self, "Очистка логов",
                                     "Удалить все сообщения старше 14 дней?\nЭто действие нельзя отменить.",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            try:
                self.logger.clean_old_logs(14)
                self.load_history()
                self.log_callback("Логи старше 14 дней удалены", "INFO")
                QMessageBox.information(self, "Успех", "Старые логи удалены")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Ошибка очистки: {str(e)}")

    def update_contacts_list(self):
        try:
            current_user = self.xmpp_username.text() if hasattr(self, 'xmpp_username') and self.xmpp_username else ""

            conn = sqlite3.connect('logs/messages.db')
            cursor = conn.cursor()

            cursor.execute('''
                SELECT DISTINCT 
                    CASE 
                        WHEN sender = ? THEN recipient
                        WHEN recipient = ? THEN sender
                    END as contact
                FROM messages 
                WHERE sender = ? OR recipient = ?
            ''', (current_user, current_user, current_user, current_user))

            contacts = set()
            for row in cursor.fetchall():
                contact = row[0]
                if contact and contact != current_user:
                    if '/' in contact:
                        contact = contact.split('/')[0]
                    contacts.add(contact)
            conn.close()

            current = self.history_contact_combo.currentText()

            self.history_contact_combo.clear()
            self.history_contact_combo.addItem("Все диалоги")
            for contact in sorted(contacts):
                self.history_contact_combo.addItem(contact)

            idx = self.history_contact_combo.findText(current)
            if idx >= 0:
                self.history_contact_combo.setCurrentIndex(idx)

        except Exception:
            pass

    def setup_chat_tab(self, tab):
        main_layout = QHBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        tab.setLayout(main_layout)

        left_panel = QWidget()
        left_panel.setMaximumWidth(240)
        left_panel.setStyleSheet("background-color: #2f3136;")
        left_layout = QVBoxLayout()
        left_panel.setLayout(left_layout)

        title = QLabel("📨 МЕССЕНДЖЕР")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 12pt; font-weight: bold; color: #ffffff; padding: 12px;")
        left_layout.addWidget(title)

        self.status_indicator = QLabel("🔴 Не подключен")
        self.status_indicator.setAlignment(Qt.AlignCenter)
        self.status_indicator.setStyleSheet("padding: 6px; font-size: 9pt;")
        left_layout.addWidget(self.status_indicator)

        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #40444b;")
        left_layout.addWidget(line)

        chats_label = QLabel("ДИАЛОГИ")
        chats_label.setStyleSheet("padding: 8px 12px; color: #8e9297; font-size: 8pt;")
        left_layout.addWidget(chats_label)

        self.chats_list = QListWidget()
        self.chats_list.itemClicked.connect(self.on_chat_selected)
        left_layout.addWidget(self.chats_list)

        new_chat_btn = QPushButton("+ Новый чат")
        new_chat_btn.clicked.connect(self.add_new_chat)
        left_layout.addWidget(new_chat_btn)

        self.delete_chat_btn = QPushButton("🗑️ Удалить диалог")
        self.delete_chat_btn.clicked.connect(self.delete_current_chat)
        self.delete_chat_btn.setEnabled(False)
        left_layout.addWidget(self.delete_chat_btn)

        left_layout.addStretch()
        main_layout.addWidget(left_panel)

        right_panel = QWidget()
        right_panel.setStyleSheet("background-color: #36393f;")
        right_layout = QVBoxLayout()
        right_panel.setLayout(right_layout)

        self.chat_header = QLabel("Выберите диалог")
        self.chat_header.setAlignment(Qt.AlignCenter)
        self.chat_header.setStyleSheet("font-size: 14pt; font-weight: bold; color: #ffffff; padding: 12px; background-color: #2f3136;")
        right_layout.addWidget(self.chat_header)

        self.chat_display = QTextEdit()
        self.chat_display.setReadOnly(True)
        self.chat_display.setObjectName("chat_display")
        right_layout.addWidget(self.chat_display)

        input_frame = QFrame()
        input_frame.setStyleSheet("background-color: #40444b; border-radius: 4px; margin: 8px;")
        input_layout = QHBoxLayout()
        input_frame.setLayout(input_layout)

        self.message_input = QLineEdit()
        self.message_input.setPlaceholderText("Введите сообщение...")
        self.message_input.setMinimumHeight(36)
        self.message_input.returnPressed.connect(self.send_chat_message)

        self.send_chat_btn = QPushButton("📨 Отправить")
        self.send_chat_btn.clicked.connect(self.send_chat_message)
        self.send_chat_btn.setEnabled(False)

        input_layout.addWidget(self.message_input)
        input_layout.addWidget(self.send_chat_btn)
        right_layout.addWidget(input_frame)

        main_layout.addWidget(right_panel)

    def delete_current_chat(self):
        if not self.current_contact:
            QMessageBox.warning(self, "Ошибка", "Выберите диалог для удаления")
            return

        reply = QMessageBox.question(self, "Удаление диалога",
                                     f"Удалить диалог с {self.current_contact}? История переписки будет удалена.",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            contact = self.current_contact

            if contact in self.chats:
                del self.chats[contact]
                self.save_chat_history()

                conn = sqlite3.connect('logs/messages.db')
                cursor = conn.cursor()
                cursor.execute("DELETE FROM messages WHERE sender LIKE ? OR recipient LIKE ?",
                               (f'%{contact}%', f'%{contact}%'))
                conn.commit()
                conn.close()

                self.current_contact = None
                self.chat_header.setText("Выберите диалог")
                self.send_chat_btn.setEnabled(False)
                self.delete_chat_btn.setEnabled(False)
                self.refresh_chats_list()
                self.chat_display.clear()
                self.log_callback(f"Диалог с {contact} удален", "INFO")
                self.load_history()
                self.update_contacts_list()

    def setup_logs_tab(self, tab):
        layout = QVBoxLayout()
        tab.setLayout(layout)

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("🔍 Фильтр:"))
        self.log_filter = QComboBox()
        self.log_filter.addItems(["Все", "Отправленные", "Полученные", "Ошибки"])
        filter_layout.addWidget(self.log_filter)
        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        self.log_text.setStyleSheet("background-color: #2f3136; color: #dcddde;")
        layout.addWidget(self.log_text)

        btn_layout = QHBoxLayout()
        export_btn = QPushButton("📁 Экспорт JSON")
        export_btn.clicked.connect(self.export_logs)
        clear_btn = QPushButton("🗑️ Очистить")
        clear_btn.clicked.connect(self.clear_logs_display)

        btn_layout.addWidget(export_btn)
        btn_layout.addWidget(clear_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

    def setup_settings_tab(self, tab):
        layout = QVBoxLayout()
        tab.setLayout(layout)

        xmpp_group = QGroupBox("Настройки XMPP подключения")
        xmpp_layout = QFormLayout()

        self.xmpp_server = QLineEdit(self.config['xmpp']['server'])
        self.xmpp_port = QSpinBox()
        self.xmpp_port.setRange(1, 65535)
        self.xmpp_port.setValue(self.config['xmpp']['port'])
        self.xmpp_username = QLineEdit(self.config['xmpp']['username'])
        self.xmpp_password = QLineEdit()
        self.xmpp_password.setEchoMode(QLineEdit.Password)
        self.xmpp_password.setText(self.config['xmpp']['password'])

        xmpp_layout.addRow("🌐 Сервер:", self.xmpp_server)
        xmpp_layout.addRow("🔌 Порт:", self.xmpp_port)
        xmpp_layout.addRow("👤 Логин:", self.xmpp_username)
        xmpp_layout.addRow("🔑 Пароль:", self.xmpp_password)

        xmpp_group.setLayout(xmpp_layout)
        layout.addWidget(xmpp_group)

        rest_group = QGroupBox("Настройки REST API")
        rest_layout = QFormLayout()

        self.rest_host = QLineEdit(self.config['rest_api']['host'])
        self.rest_port = QSpinBox()
        self.rest_port.setRange(1, 65535)
        self.rest_port.setValue(self.config['rest_api']['port'])
        self.rest_endpoint = QLineEdit(self.config['rest_api']['endpoint'])
        self.rest_api_key = QLineEdit(self.config['rest_api'].get('api_key', ''))
        self.rest_api_key.setEchoMode(QLineEdit.Password)
        self.rest_api_key.setPlaceholderText("Пусто = без авторизации (не рекомендуется для 0.0.0.0)")

        self.verify_tls = QCheckBox("Проверять TLS-сертификат")
        self.verify_tls.setChecked(self.config['xmpp'].get('verify_tls', False))

        xmpp_layout.addRow(self.verify_tls)

        rest_layout.addRow("🏠 Хост:", self.rest_host)
        rest_layout.addRow("🔌 Порт:", self.rest_port)
        rest_layout.addRow("📡 Endpoint:", self.rest_endpoint)
        rest_layout.addRow("🔐 API ключ:", self.rest_api_key)

        rest_group.setLayout(rest_layout)
        layout.addWidget(rest_group)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)

        self.connect_btn = QPushButton("🔌 ПОДКЛЮЧИТЬСЯ")
        self.connect_btn.clicked.connect(self.connect_xmpp)

        self.register_btn = QPushButton("👤 Создать пользователя")
        self.register_btn.clicked.connect(self.open_register_dialog)
        self.register_btn.setEnabled(False)

        self.start_rest_btn = QPushButton("🚀 ЗАПУСТИТЬ REST")
        self.start_rest_btn.clicked.connect(self.start_rest)

        self.save_btn = QPushButton("💾 Сохранить")
        self.save_btn.clicked.connect(self.save_settings)

        btn_layout.addWidget(self.connect_btn)
        btn_layout.addWidget(self.register_btn)
        btn_layout.addWidget(self.start_rest_btn)
        btn_layout.addWidget(self.save_btn)
        btn_layout.addStretch()

        layout.addLayout(btn_layout)

        status_frame = QFrame()
        status_frame.setStyleSheet("background-color: #40444b; border-radius: 4px;")
        status_layout = QVBoxLayout()
        self.connection_status = QLabel("📡 Статус: Ожидание")
        self.rest_status = QLabel("🌐 REST API: Остановлен")
        status_layout.addWidget(self.connection_status)
        status_layout.addWidget(self.rest_status)
        status_frame.setLayout(status_layout)
        layout.addWidget(status_frame)

        layout.addStretch()

    def setup_info_tab(self, tab):
        layout = QVBoxLayout()
        tab.setLayout(layout)

        info_text = QTextEdit()
        info_text.setReadOnly(True)
        info_text.setStyleSheet("background-color: #2f3136; color: #dcddde;")
        info_text.setHtml("""
        <h1 style="color: #7289da;">📨 XMPP Мессенджер</h1>
        <p><b>Версия:</b> 2.0</p>
        <p><b>Протокол:</b> XMPP (Jabber)</p>
        <p><b>API:</b> REST</p>
        <h2 style="color: #7289da;">⚙️ Функции:</h2>
        <ul>
            <li>Полноценный чат с историей</li>
            <li>Статусы сообщений: ✓ доставлено, ✓✓ прочитано</li>
            <li>Подключение к XMPP серверу</li>
            <li>REST API для внешних систем</li>
            <li>Создание новых пользователей</li>
            <li>История сообщений с фильтром по контактам</li>
            <li>Автоочистка логов каждые 14 дней</li>
            <li>Удаление диалогов</li>
        </ul>
        """)
        layout.addWidget(info_text)

    def save_settings(self):
        self.config['xmpp']['server'] = self.xmpp_server.text()
        self.config['xmpp']['port'] = self.xmpp_port.value()
        self.config['xmpp']['username'] = self.xmpp_username.text()
        self.config['xmpp']['password'] = self.xmpp_password.text()
        self.config['rest_api']['host'] = self.rest_host.text()
        self.config['rest_api']['port'] = self.rest_port.value()
        self.config['rest_api']['endpoint'] = self.rest_endpoint.text()
        self.config['rest_api']['api_key'] = self.rest_api_key.text().strip()
        self.config['xmpp']['verify_tls'] = self.verify_tls.isChecked()
        self.persist_settings()
        self.log_callback("Настройки сохранены", "INFO")
        QMessageBox.information(self, "Успех", "Настройки сохранены")

    def add_new_chat(self):
        contact, ok = QInputDialog.getText(self, "Новый чат",
                                            "Введите JID пользователя (например: username@arsenal):")
        if ok and contact:
            if contact not in self.chats:
                self.chats[contact] = []
                self.save_chat_history()
                self.refresh_chats_list()
                self.log_callback(f"Начат чат с {contact}", "INFO")
                self.current_contact = contact
                self.chat_header.setText(f"💬 {contact}")
                self.send_chat_btn.setEnabled(True)
                self.delete_chat_btn.setEnabled(True)
                self.refresh_chat_display()
            else:
                QMessageBox.warning(self, "Внимание", "Чат с этим пользователем уже существует")

    def on_chat_selected(self, item):
        contact = item.data(Qt.UserRole)
        self.current_contact = contact
        self.chat_header.setText(f"💬 {contact}")
        self.send_chat_btn.setEnabled(True)
        self.delete_chat_btn.setEnabled(True)
        self.mark_messages_as_read(contact)
        self.refresh_chat_display()
        self.message_input.setFocus()

    def mark_messages_as_read(self, contact):
        """Отметить сообщения как прочитанные и отправить подтверждение"""
        if contact in self.chats:
            updated = False
            for msg in self.chats[contact]:
                if msg['type'] == 'received' and not msg.get('read', False):
                    msg['read'] = True
                    updated = True
            if updated:
                self.save_chat_history()
                self.refresh_chats_list()
                if self.current_contact == contact:
                    self.refresh_chat_display()

                try:
                    if (
                        self.xmpp_service
                        and self.xmpp_service.client
                        and self.xmpp_service.client.is_connected
                    ):
                        last_incoming_id = None
                        for msg in reversed(self.chats.get(contact, [])):
                            if msg.get('type') == 'received' and msg.get('msg_id'):
                                last_incoming_id = msg['msg_id']
                                break
                        if last_incoming_id:
                            self.xmpp_service.client.send_displayed_marker(
                                contact, last_incoming_id
                            )
                            self.log_callback(
                                f"✅ Отправлено подтверждение прочтения для {contact}", "INFO"
                            )
                except Exception as e:
                    self.log_callback(f"Ошибка отправки подтверждения: {str(e)}", "ERROR")

    def refresh_chats_list(self):
        self.chats_list.clear()
        for contact in self.chats.keys():
            item = QListWidgetItem()
            last_msg = ""
            if self.chats[contact]:
                last_msg = self.chats[contact][-1]['message'][:30]
            unread = self.get_unread_count(contact)

            if unread > 0:
                item.setText(f"📩 {contact}\n{last_msg}")
                item.setForeground(QColor(114, 137, 218))
            else:
                item.setText(f"💬 {contact}\n{last_msg}")

            item.setData(Qt.UserRole, contact)
            self.chats_list.addItem(item)

    def get_unread_count(self, contact):
        if contact not in self.chats:
            return 0
        return sum(1 for msg in self.chats[contact]
                  if msg.get('type') == 'received' and not msg.get('read', False))

    def refresh_chat_display(self):
        if not self.current_contact or self.current_contact not in self.chats:
            self.chat_display.clear()
            return

        self.chat_display.clear()

        for msg in self.chats[self.current_contact]:
            safe_text = html.escape(msg['message'])
            if msg['type'] == 'sent':
                if msg.get('status') == 'read':
                    status_icon = "✓✓"
                    status_color = "#43b581"
                elif msg.get('status') == 'delivered':
                    status_icon = "✓"
                    status_color = "#8e9297"
                elif msg.get('status') == 'failed':
                    status_icon = "✗"
                    status_color = "#f04747"
                else:
                    status_icon = "⏳"
                    status_color = "#faa81a"

                self.chat_display.append(
                    f"<div style='text-align: right; margin: 8px 12px;'>"
                    f"<div style='background-color: #40444b; color: #dcddde; display: inline-block; "
                    f"padding: 8px 14px; border-radius: 18px; max-width: 70%; text-align: left; font-size: 9pt;'>"
                    f"{safe_text}<br>"
                    f"<span style='font-size: 7pt; color: #8e9297;'>{msg['time']} "
                    f"<span style='color: {status_color};'>{status_icon}</span></span>"
                    f"</div></div>"
                )
            else:
                self.chat_display.append(
                    f"<div style='text-align: left; margin: 8px 12px;'>"
                    f"<div style='background-color: #2f3136; color: #dcddde; display: inline-block; "
                    f"padding: 8px 14px; border-radius: 18px; max-width: 70%; text-align: left; font-size: 9pt;'>"
                    f"{safe_text}<br>"
                    f"<span style='font-size: 7pt; color: #8e9297;'>{msg['time']}</span>"
                    f"</div></div>"
                )
                if not msg.get('read', False):
                    msg['read'] = True
                    self.save_chat_history()

        scrollbar = self.chat_display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        self.refresh_chats_list()

    def send_chat_message(self):
        if not self.current_contact:
            QMessageBox.warning(self, "Ошибка", "Выберите чат")
            return

        if not self.xmpp_service or not self.xmpp_service.client.is_connected:
            QMessageBox.warning(self, "Ошибка", "Нет подключения к XMPP серверу")
            return

        message = self.message_input.text().strip()
        if not message:
            return

        try:
            message = validate_message(message)
        except ValueError as exc:
            QMessageBox.warning(self, "Ошибка", str(exc))
            return

        try:
            msg_id = str(uuid.uuid4())

            if self.current_contact not in self.chats:
                self.chats[self.current_contact] = []

            self.chats[self.current_contact].append({
                'type': 'sent',
                'message': message,
                'time': datetime.now().strftime("%H:%M:%S"),
                'status': 'sending',
                'msg_id': msg_id
            })
            self.pending_messages[msg_id] = self.current_contact
            self.save_chat_history()
            self.refresh_chat_display()
            self.message_input.clear()

            self.xmpp_service.client.queue_message(
                self.current_contact, message, msg_id=msg_id
            )
            self.log_callback(f"Сообщение поставлено в очередь: {self.current_contact}", "INFO")

        except Exception as e:
            self.update_message_status(msg_id, 'failed')
            QMessageBox.critical(self, "Ошибка", f"Не удалось отправить: {str(e)}")

    def update_message_status(self, msg_id, status):
        contact = self.pending_messages.get(msg_id)
        if contact and contact in self.chats:

            target_idx = None
            for i, msg in enumerate(self.chats[contact]):
                if msg.get('msg_id') == msg_id and msg['type'] == 'sent':
                    target_idx = i
                    break
            if target_idx is None:
                return
            changed = False

            if status == 'read':
                for msg in self.chats[contact][:target_idx + 1]:
                    if msg['type'] == 'sent':
                        current = msg.get('status', 'sending')
                        if current in ('sending', 'delivered'):
                            msg['status'] = 'read'
                            changed = True
            else:
                msg = self.chats[contact][target_idx]
                current = msg.get('status', 'sending')
                if status == 'delivered' and current in ('sending',):
                    msg['status'] = status
                    changed = True
                elif status == 'failed':
                    msg['status'] = status
                    changed = True
            if changed:
                self.save_chat_history()
                if self.current_contact == contact:
                    self.refresh_chat_display()

    # --- Публичные обработчики, вызываемые из потоков (только emit!) ---

    def on_delivery_received(self, msg_id):
        self.sig_delivery_received.emit(msg_id)

    def on_send_failed(self, msg_id):
        self.sig_send_failed.emit(msg_id)

    def on_receipt_received(self, msg_id):
        self.sig_receipt_received.emit(msg_id)

    def receive_message(self, from_jid, message, msg_id=None):
        self.sig_received_message.emit(from_jid, message, msg_id or "")

    def log_callback(self, message, level="INFO"):
        self.sig_log.emit(message, level)

    # --- Слоты главного потока ---

    @pyqtSlot(str)
    def _on_send_failed_safe(self, msg_id):
        self.logger.mark_failed(msg_id)
        self.update_message_status(msg_id, 'failed')
        self.log_callback(f"Не удалось отправить сообщение: {msg_id}", "ERROR")

    @pyqtSlot(str)
    def _on_delivery_received_safe(self, msg_id):
        self.logger.mark_delivered(msg_id)
        self.log_callback(f"✓ Доставлено: {msg_id}", "INFO")
        self.update_message_status(msg_id, 'delivered')

    @pyqtSlot(str)
    def _on_receipt_received_safe(self, msg_id):
        self.log_callback(f'DEBUG read: msg_id={msg_id} pending={list(self.pending_messages.keys())[:5]}', 'INFO')
        self.logger.mark_read(msg_id)
        self.log_callback(f"✓✓ Прочитано: {msg_id}", "INFO")
        self.update_message_status(msg_id, 'read')

    @pyqtSlot(str, str, str)
    def _on_message_sent_safe(self, to_user, message, msg_id):
        """Слот главного потока: обновляет GUI после отправки через REST API."""
        self.refresh_chats_list()
        if self.current_contact == to_user:
            self.refresh_chat_display()
        self._history_dirty = True

    @pyqtSlot(str, str, str)
    def _process_received_message(self, from_jid, message, msg_id):
        try:
            if from_jid not in self.chats:
                self.chats[from_jid] = []

            if not msg_id:
                msg_id = str(int(datetime.now().timestamp() * 1000))
            self.chats[from_jid].append({
                'type': 'received',
                'message': message,
                'time': datetime.now().strftime("%H:%M:%S"),
                'read': False,
                'msg_id': msg_id
            })
            self.save_chat_history()

            self.log_callback(f"📩 Получено сообщение от {from_jid}: {message}", "INFO")

            # Отложенное обновление истории (не дёргаем GUI каждое сообщение)
            self._history_dirty = True
            self.refresh_chats_list()

            if self.current_contact == from_jid:
                self._chat_dirty = True
                if msg_id and self.xmpp_service and self.xmpp_service.client and self.xmpp_service.client.is_connected:
                    self.xmpp_service.client.send_displayed_marker(from_jid, msg_id)
            else:
                self.status_label.setText(f"📩 Новое сообщение от {from_jid}")
                QTimer.singleShot(3000, lambda: self.status_label.setText(f"🟢 Подключено"))
        except Exception as e:
            self.log_callback(f"Ошибка обработки сообщения: {str(e)}", "ERROR")

    def connect_xmpp(self):
        username = self.xmpp_username.text().strip()
        password = self.xmpp_password.text()

        if not username or not password:
            QMessageBox.warning(self, "Ошибка", "Введите логин и пароль в настройках")
            return

        if getattr(self, "_connect_worker", None) and self._connect_worker.isRunning():
            return

        self.connect_btn.setText("⏳ ПОДКЛЮЧЕНИЕ...")
        self.connect_btn.setEnabled(False)
        self.log_callback("Подключение к XMPP...", "INFO")

        self._connect_worker = XmppConnectWorker(
            self.config, self.log_callback, username, password, self.kafka_broker
        )
        self._connect_worker.finished.connect(self._on_xmpp_connect_finished)
        self._connect_worker.start()

    def _on_xmpp_connect_finished(self, success: bool, error: str):
        username = self.xmpp_username.text().strip()

        if success and self._connect_worker.service:
            self.xmpp_service = self._connect_worker.service
            self.status_indicator.setText("🟢 Подключен")
            self.status_indicator.setStyleSheet("padding: 6px; color: #43b581;")
            self.status_label.setText(f"🟢 Подключено: {username}")
            self.connect_btn.setText("✅ ПОДКЛЮЧЕНО")
            self.log_callback("Подключено к XMPP серверу", "INFO")

            self.xmpp_service.client.on_message_received = self.receive_message
            self.xmpp_service.client.on_delivery_received = self.on_delivery_received
            self.xmpp_service.client.on_read_received = self.on_receipt_received
            self.xmpp_service.client.on_send_failed = self.on_send_failed

            self.register_btn.setEnabled(True)
            self.update_contacts_list()
        else:
            self.connect_btn.setText("🔌 ПОДКЛЮЧИТЬСЯ")
            self.connect_btn.setEnabled(True)
            msg = error or "Не удалось подключиться к серверу"
            self.log_callback(msg, "ERROR")
            QMessageBox.critical(self, "Ошибка", msg)

    def open_register_dialog(self):
        if not self.xmpp_service or not self.xmpp_service.client.is_connected:
            QMessageBox.warning(self, "Ошибка", "Сначала подключитесь к XMPP серверу")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Регистрация нового пользователя")
        dialog.setGeometry(200, 200, 400, 300)
        dialog.setStyleSheet("background-color: #2f3136; color: #dcddde;")

        layout = QVBoxLayout()

        form_layout = QFormLayout()

        username_edit = QLineEdit()
        username_edit.setPlaceholderText("логин")
        password_edit = QLineEdit()
        password_edit.setEchoMode(QLineEdit.Password)
        password_edit.setPlaceholderText("пароль")
        confirm_edit = QLineEdit()
        confirm_edit.setEchoMode(QLineEdit.Password)
        confirm_edit.setPlaceholderText("повторите пароль")
        email_edit = QLineEdit()
        email_edit.setPlaceholderText("email (необязательно)")

        form_layout.addRow("👤 Логин:", username_edit)
        form_layout.addRow("🔑 Пароль:", password_edit)
        form_layout.addRow("🔑 Подтверждение:", confirm_edit)
        form_layout.addRow("📧 Email:", email_edit)

        layout.addLayout(form_layout)

        btn_layout = QHBoxLayout()
        register_btn = QPushButton("✅ Зарегистрировать")
        cancel_btn = QPushButton("❌ Отмена")

        btn_layout.addWidget(register_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        dialog.setLayout(layout)

        def do_register():
            username = username_edit.text().strip()
            password = password_edit.text()
            confirm = confirm_edit.text()
            email = email_edit.text().strip()

            if not username or not password:
                QMessageBox.warning(dialog, "Ошибка", "Введите логин и пароль")
                return

            if password != confirm:
                QMessageBox.warning(dialog, "Ошибка", "Пароли не совпадают")
                return

            if len(password) < 4:
                QMessageBox.warning(dialog, "Ошибка", "Пароль должен быть не менее 4 символов")
                return

            try:
                success = self.xmpp_service.register_user(username, password, email)
                if success:
                    self.log_callback(f"Запрос на регистрацию пользователя {username} отправлен", "INFO")
                    QMessageBox.information(dialog, "Успех", f"Запрос на регистрацию пользователя {username} отправлен.\nЕсли сервер поддерживает регистрацию, пользователь будет создан.")
                    dialog.accept()
                else:
                    QMessageBox.critical(dialog, "Ошибка", "Не удалось отправить запрос регистрации")
            except Exception as e:
                QMessageBox.critical(dialog, "Ошибка", f"Ошибка: {str(e)}")

        register_btn.clicked.connect(do_register)
        cancel_btn.clicked.connect(dialog.reject)

        dialog.exec_()

    def start_rest(self):
        try:
            if self.rest_server and self.rest_server.is_running:
                QMessageBox.information(self, "Инфо", "REST API уже запущен")
                return

            self.start_rest_btn.setText("⏳ ЗАПУСК...")
            self.start_rest_btn.setEnabled(False)
            self.start_rest_btn.repaint()

            def message_handler(to_user, message, priority="normal", msg_id=None):
                """Вызывается из потока REST-сервера. GUI трогать нельзя!"""
                try:
                    if self.xmpp_service is None:
                        self.log_callback("XMPP не подключен", "ERROR")
                        return False
                    if self.xmpp_service.client is None:
                        self.log_callback("XMPP клиент не создан", "ERROR")
                        return False
                    if not self.xmpp_service.client.is_connected:
                        self.log_callback("XMPP не подключен к серверу", "ERROR")
                        return False

                    msg_id = msg_id or str(uuid.uuid4())

                    # Работа с chats — под локом (не GUI)
                    with self.chats_lock:
                        if to_user not in self.chats:
                            self.chats[to_user] = []
                        self.chats[to_user].append({
                            'type': 'sent',
                            'message': message,
                            'time': datetime.now().strftime("%H:%M:%S"),
                            'status': 'sending',
                            'msg_id': msg_id
                        })
                    with self.pending_messages_lock:
                        self.pending_messages[msg_id] = to_user

                    # Файл — можно из потока
                    self.save_chat_history()

                    # Отправка через XMPP
                    result = self.xmpp_service.send_message(
                        to_user, message, msg_id=msg_id, priority=priority
                    )

                    # GUI — ТОЛЬКО через сигнал!
                    self.sig_message_sent.emit(to_user, message, msg_id)

                    return msg_id if result is not None else None
                except Exception as e:
                    self.log_callback(f"Ошибка в message_handler: {str(e)}", "ERROR")
                    return False

            self.rest_server = RESTServer(
                self.config, message_handler, self.log_callback, self.kafka_broker,
                message_logger=self.logger,
            )
            self.rest_server.start()

            self.start_rest_btn.setText("✅ ЗАПУЩЕНО")
            self.rest_status.setText(f"✅ REST API: Запущен на порту {self.config['rest_api']['port']}")
            self.log_callback(f"REST API запущен на порту {self.config['rest_api']['port']}", "INFO")
            QMessageBox.information(self, "Успех", f"REST API запущен на порту {self.config['rest_api']['port']}")

        except Exception as e:
            self.start_rest_btn.setText("🚀 ЗАПУСТИТЬ REST")
            self.start_rest_btn.setEnabled(True)
            self.log_callback(f"Ошибка запуска REST: {str(e)}", "ERROR")
            QMessageBox.critical(self, "Ошибка", f"Ошибка: {str(e)}")

    def export_logs(self):
        try:
            filepath = self.logger.export_to_json()
            QMessageBox.information(self, "Успех", f"Логи экспортированы в:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка экспорта: {str(e)}")

    def clear_logs_display(self):
        self.log_text.clear()

    def check_for_updates(self):
        try:
            from updater import check_for_updates
            def on_update_check(has_update, latest_version, download_url):
                if has_update:
                    self.sig_log.emit(f"__UPDATE__:{latest_version}:{download_url or ''}", "INFO")
            check_for_updates(on_update_check)
        except Exception:
            pass

    @pyqtSlot(str, str)
    def _show_update_dialog(self, latest_version, download_url):
        from version import VERSION
        msg = QMessageBox(self)
        msg.setWindowTitle("Обновление доступно")
        msg.setText(f"Доступна новая версия {latest_version} (текущая: {VERSION}).\nХотите скачать?")
        msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        if msg.exec_() == QMessageBox.Yes:
            import webbrowser
            webbrowser.open(download_url if download_url else "https://github.com/Ottiks17/XMPP2/releases/latest")

    def auto_start(self):
        username = self.xmpp_username.text()
        password = self.xmpp_password.text()

        if username and password:
            self.log_callback("Автоматическое подключение к XMPP...", "INFO")
            self.connect_xmpp()
            QTimer.singleShot(3000, self.check_and_start_rest)
        else:
            self.log_callback("Настройки не сохранены. Автозапуск отключен.", "WARNING")

    def check_and_start_rest(self):
        if self.xmpp_service and self.xmpp_service.client and self.xmpp_service.client.is_connected:
            self.log_callback("Автоматический запуск REST API...", "INFO")
            self.start_rest()
        else:
            self.log_callback("XMPP не подключен, REST API не запущен", "WARNING")

    @pyqtSlot(str, str)
    def _log_callback_safe(self, message, level):
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Специальный маркер для диалога обновления (не пишется в лог)
        if message.startswith("__UPDATE__:"):
            try:
                _, version, url = message.split(":", 2)
                self._show_update_dialog(version, url)
            except ValueError:
                pass
            return

        display_message = message
        if level == "MESSAGE":
            try:
                msg_data = json.loads(message)
                display_message = (
                    f"{msg_data.get('type', '?')} "
                    f"{msg_data.get('sender', '?')} → {msg_data.get('recipient', '?')}: "
                    f"{msg_data.get('message', '')[:80]}"
                )
                self.logger.log_message(
                    msg_type=msg_data['type'],
                    sender=msg_data['sender'],
                    recipient=msg_data['recipient'],
                    message=msg_data['message'],
                    message_id=msg_data.get('message_id'),
                    priority=msg_data.get('priority'),
                    send_time=datetime.fromisoformat(msg_data['send_time'])
                    if msg_data.get('send_time')
                    else None,
                    delivery_time=datetime.fromisoformat(msg_data['delivery_time'])
                    if msg_data.get('delivery_time')
                    else None,
                    read_time=datetime.fromisoformat(msg_data['read_time'])
                    if msg_data.get('read_time')
                    else None,
                )
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                display_message = message

        color = "#dcddde"
        if level == "ERROR":
            color = "#f04747"
        elif level == "MESSAGE":
            color = "#43b581"

        safe_display = html.escape(display_message)
        log_entry = (
            f'<span style="color: #8e9297;">[{timestamp}]</span> '
            f'<span style="color: {color};">[{level}]</span> {safe_display}<br>'
        )

        self.log_text.append(log_entry)
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

        try:
            with open("logs/app.log", "a", encoding='utf-8') as f:
                f.write(f"[{timestamp}] [{level}] {display_message}\n")
        except OSError:
            pass

    def closeEvent(self, event):
        self.save_chat_history()
        if self.xmpp_service:
            self.xmpp_service.disconnect()
        if self.rest_server:
            self.rest_server.stop()
        event.accept()


class XmppConnectWorker(QThread):
    finished = pyqtSignal(bool, str)

    def __init__(self, config, log_callback, username, password, kafka_broker=None):
        super().__init__()
        self.config = config
        self.log_callback = log_callback
        self.username = username
        self.password = password
        self.kafka_broker = kafka_broker
        self.service = None

    def run(self):
        try:
            self.service = XMPPService(self.config, self.log_callback, self.kafka_broker)
            ok = self.service.connect(self.username, self.password)
            self.finished.emit(ok, "")
        except Exception as exc:
            self.finished.emit(False, str(exc))


def main():
    app = QApplication(sys.argv)
    window = XMPPGUI()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()