# -*- coding: utf-8 -*-
"""
XMPP Мессенджер — десктопный GUI на PyQt5 (дизайн в стиле Telegram Desktop).

Вся XMPP/REST/Kafka/история-логика (XMPPGUI, XmppConnectWorker, message_handler
внутри start_rest, работа с self.chats/self.chats_lock/self.pending_messages,
обработка межпоточных сигналов) перенесена из предыдущей версии этого файла
БЕЗ ИЗМЕНЕНИЙ. Переписан только слой отображения: вместо QTabWidget теперь
список чатов + переписка в стиле Telegram, с диалогами Настроек/Истории/Логов,
открываемыми по требованию из меню гамбургера (см. XMPPGUI.init_ui).

Файл построен из нескольких логических разделов (помечены заголовками ниже),
раньше они были отдельными модулями пакета ui/ — теперь всё в одном файле
gui_app.py по прямому указанию: тема оформления, делегаты/виджеты списка
чатов и переписки, панель профиля, диалоги настроек/истории/логов и, в самом
конце, собственно класс XMPPGUI.

ЧТО СОХРАНЕНО БЕЗ ИЗМЕНЕНИЙ (имена, сигнатуры, поведение):
  Сигналы: sig_received_message, sig_delivery_received, sig_receipt_received,
           sig_send_failed, sig_log, sig_message_sent.
  Атрибуты: self.chats, self.chats_lock, self.current_contact,
           self.pending_messages, self.pending_messages_lock,
           self.xmpp_service, self.rest_server, self.kafka_broker,
           self.logger, self.config.
  Методы (имя + внешнее поведение): connect_xmpp, send_chat_message,
           receive_message, on_delivery_received, on_receipt_received,
           on_send_failed, log_callback, refresh_chats_list,
           refresh_chat_display, load_chat_history, save_chat_history,
           update_message_status, mark_messages_as_read,
           _process_received_message, _on_delivery_received_safe,
           _on_receipt_received_safe, _on_send_failed_safe,
           _log_callback_safe, _on_message_sent_safe.
  Класс XmppConnectWorker — скопирован дословно.
  Правило потоков: обращение к GUI из не-главного потока — ТОЛЬКО через
           emit сигнала (как и было).

ЧТО ИЗМЕНИЛОСЬ ПО СУЩЕСТВУ (сознательно, не баг) — подробности см. в
докстринге класса XMPPGUI ниже, перед init_ui().
"""

import hashlib
import html
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime
from threading import Lock

from PyQt5.QtCore import (
    QAbstractListModel,
    QEasingCurve,
    QModelIndex,
    QPointF,
    QPropertyAnimation,
    QRect,
    QRectF,
    QSize,
    Qt,
    QThread,
    QTimer,
    pyqtSignal,
    pyqtSlot,
)
from PyQt5.QtGui import (
    QColor,
    QFont,
    QFontMetrics,
    QPainter,
    QPainterPath,
    QPixmap,
    QStandardItem,
    QStandardItemModel,
    QTextLayout,
    QTextOption,
)
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QStatusBar,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app.config import load_config, save_config as write_config
from app.constants import CHAT_HISTORY_PATH, MAX_MESSAGE_LENGTH
from app.validation import validate_message
from app.xmpp.service import XMPPService
from app.kafka_broker import KafkaBroker
from rest_server import RESTServer
from app.storage import MessageLogger

os.environ['LANG'] = 'ru_RU.UTF-8'


# ======================================================================
# 1. ТЕМА ОФОРМЛЕНИЯ (было ui/theme.py)
# ======================================================================

TELEGRAM_DARK: dict[str, str] = {
    "bg_primary": "#17212b",     # фон окна чата
    "bg_secondary": "#0e1621",   # левая панель (список чатов)
    "bg_input": "#17212b",       # фон поля ввода / шапки
    "bg_hover": "#202b36",       # ховер по элементу списка / кнопке
    "bg_selected": "#2b5278",    # выбранный чат в списке
    "bubble_sent": "#2b5278",    # пузырь исходящего сообщения
    "bubble_recv": "#182533",    # пузырь входящего сообщения
    "accent": "#5288c1",         # акцентный цвет (ссылки, активные элементы)
    "text_primary": "#ffffff",   # основной текст
    "text_secondary": "#7f91a4", # вторичный текст (время, статус, превью)
    "divider": "#101921",        # разделительные линии
    "online": "#4dcd5e",         # индикатор "в сети"
    "error": "#e53935",          # ошибки, недоставленные сообщения
}


def build_qss() -> str:
    """Собирает единый QSS для всего приложения из TELEGRAM_DARK.

    Возвращает готовую строку для self.setStyleSheet(). Все цвета берутся
    только из TELEGRAM_DARK — при смене темы достаточно поменять словарь.
    """
    c = TELEGRAM_DARK

    return f"""
    /* ---------- Базовые контейнеры ---------- */
    QMainWindow#MainWindow {{
        background-color: {c['bg_primary']};
    }}

    QWidget {{
        background-color: {c['bg_primary']};
        color: {c['text_primary']};
        font-family: "Segoe UI", "Noto Sans", sans-serif;
        font-size: 10pt;
    }}

    QSplitter::handle {{
        background-color: {c['divider']};
        width: 1px;
    }}

    /* ---------- Левая панель (список чатов) ---------- */
    QWidget#Sidebar {{
        background-color: {c['bg_secondary']};
        border: none;
    }}

    QToolButton#SidebarMenuButton,
    QToolButton#SidebarNewChatButton {{
        background: transparent;
        border: none;
        border-radius: 18px;
        padding: 6px;
    }}
    QToolButton#SidebarMenuButton:hover,
    QToolButton#SidebarNewChatButton:hover {{
        background-color: {c['bg_hover']};
    }}

    QLineEdit#SidebarSearch {{
        background-color: {c['bg_hover']};
        border: none;
        border-radius: 16px;
        padding: 6px 12px;
        color: {c['text_primary']};
        selection-background-color: {c['accent']};
    }}
    QLineEdit#SidebarSearch:focus {{
        border: 1px solid {c['accent']};
    }}

    QListView#ChatListView {{
        background-color: {c['bg_secondary']};
        border: none;
        outline: none;
    }}
    /* Фон hover/selected для строк списка рисует ChatListDelegate вручную
       (нужен полный контроль над формой аватара и бейджа), поэтому здесь
       item-селекторы намеренно не заданы. */

    /* ---------- Правая панель (чат) ---------- */
    QWidget#ChatView {{
        background-color: {c['bg_primary']};
    }}

    QWidget#ChatHeader {{
        background-color: {c['bg_input']};
        border-bottom: 1px solid {c['divider']};
    }}

    QLabel#ChatHeaderName {{
        font-weight: bold;
        font-size: 11pt;
        color: {c['text_primary']};
    }}

    QLabel#ChatHeaderStatus {{
        font-size: 9pt;
        color: {c['text_secondary']};
    }}

    QListView#MessageListView {{
        background-color: {c['bg_primary']};
        border: none;
        outline: none;
    }}
    /* Пузыри сообщений рисует MessageDelegate вручную (см. docstring выше:
       выравнивание, скругления, галочки статуса) — здесь только фон вьюпорта. */

    /* ---------- Поле ввода ---------- */
    QWidget#InputBar {{
        background-color: {c['bg_input']};
        border-top: 1px solid {c['divider']};
    }}

    QTextEdit#MessageInput {{
        background-color: {c['bg_hover']};
        border: none;
        border-radius: 18px;
        padding: 8px 14px;
        color: {c['text_primary']};
        selection-background-color: {c['accent']};
    }}

    QPushButton#SendButton {{
        background-color: {c['accent']};
        border: none;
        border-radius: 18px;
        color: {c['text_primary']};
        font-weight: bold;
    }}
    QPushButton#SendButton:hover {{
        background-color: #619bd6;
    }}
    QPushButton#SendButton:disabled {{
        background-color: {c['bg_hover']};
        color: {c['text_secondary']};
    }}

    /* ---------- Кнопки-иконки без рамки (flat=true) ---------- */
    *[flat="true"] {{
        background: transparent;
        border: none;
        border-radius: 16px;
        padding: 6px;
        color: {c['text_secondary']};
    }}
    *[flat="true"]:hover {{
        background-color: {c['bg_hover']};
        color: {c['text_primary']};
    }}

    /* ---------- Панель профиля ---------- */
    QWidget#ProfilePanel {{
        background-color: {c['bg_secondary']};
        border-left: 1px solid {c['divider']};
    }}

    QLabel#ProfileJid {{
        color: {c['text_secondary']};
        font-size: 9pt;
    }}

    QPushButton#ProfileDeleteButton {{
        background: transparent;
        border: none;
        color: {c['error']};
        text-align: left;
        padding: 10px 16px;
    }}
    QPushButton#ProfileDeleteButton:hover {{
        background-color: {c['bg_hover']};
    }}

    /* ---------- Диалоги (Настройки / История / Логи) ---------- */
    QDialog {{
        background-color: {c['bg_primary']};
    }}

    QListWidget#SettingsNav {{
        background-color: {c['bg_secondary']};
        border: none;
        border-right: 1px solid {c['divider']};
        outline: none;
    }}
    QListWidget#SettingsNav::item {{
        padding: 10px 14px;
        border-radius: 0;
    }}
    QListWidget#SettingsNav::item:selected {{
        background-color: {c['bg_selected']};
        color: {c['text_primary']};
    }}
    QListWidget#SettingsNav::item:hover:!selected {{
        background-color: {c['bg_hover']};
    }}

    QStackedWidget#SettingsStack {{
        background-color: {c['bg_primary']};
    }}

    /* ---------- Общие элементы форм (используются в SettingsDialog) ---------- */
    QLineEdit, QSpinBox, QComboBox {{
        background-color: {c['bg_hover']};
        border: 1px solid {c['divider']};
        border-radius: 6px;
        padding: 6px 10px;
        color: {c['text_primary']};
        selection-background-color: {c['accent']};
    }}
    QLineEdit:focus, QSpinBox:focus, QComboBox:focus {{
        border: 1px solid {c['accent']};
    }}

    QCheckBox {{
        color: {c['text_primary']};
        spacing: 8px;
    }}

    QLabel {{
        color: {c['text_primary']};
    }}

    /* ---------- Кнопки общего назначения ---------- */
    QPushButton {{
        background-color: {c['bg_hover']};
        border: none;
        border-radius: 6px;
        padding: 8px 16px;
        color: {c['text_primary']};
    }}
    QPushButton:hover {{
        background-color: #26333f;
    }}
    QPushButton:pressed {{
        background-color: {c['bg_selected']};
    }}
    QPushButton:disabled {{
        color: {c['text_secondary']};
    }}

    QPushButton[class="primary"] {{
        background-color: {c['accent']};
        font-weight: bold;
    }}
    QPushButton[class="primary"]:hover {{
        background-color: #619bd6;
    }}

    QPushButton[class="secondary"] {{
        background-color: transparent;
        border: 1px solid {c['divider']};
        color: {c['text_secondary']};
    }}
    QPushButton[class="secondary"]:hover {{
        background-color: {c['bg_hover']};
        color: {c['text_primary']};
    }}

    /* ---------- Таблицы (HistoryDialog) ---------- */
    QTableView, QTableWidget {{
        background-color: {c['bg_primary']};
        alternate-background-color: {c['bg_secondary']};
        gridline-color: {c['divider']};
        border: 1px solid {c['divider']};
        selection-background-color: {c['bg_selected']};
        color: {c['text_primary']};
    }}
    QTableWidget::item, QTableView::item {{
        background-color: transparent;
        color: {c['text_primary']};
        padding: 4px 6px;
    }}
    QTableWidget::item:selected, QTableView::item:selected {{
        background-color: {c['bg_selected']};
        color: {c['text_primary']};
    }}
    QHeaderView::section {{
        background-color: {c['bg_secondary']};
        color: {c['text_secondary']};
        padding: 6px;
        border: none;
        border-bottom: 1px solid {c['divider']};
    }}

    /* ---------- Логи (LogsDialog) ---------- */
    QPlainTextEdit, QTextEdit#LogsView {{
        background-color: {c['bg_secondary']};
        border: 1px solid {c['divider']};
        color: {c['text_primary']};
        font-family: "Consolas", "Courier New", monospace;
    }}

    /* ---------- Скроллбары (везде) ---------- */
    QScrollBar:vertical {{
        background: transparent;
        width: 10px;
        margin: 0;
    }}
    QScrollBar::handle:vertical {{
        background: {c['bg_hover']};
        border-radius: 5px;
        min-height: 24px;
    }}
    QScrollBar::handle:vertical:hover {{
        background: {c['bg_selected']};
    }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
        height: 0;
    }}
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
        background: none;
    }}

    QScrollBar:horizontal {{
        background: transparent;
        height: 10px;
        margin: 0;
    }}
    QScrollBar::handle:horizontal {{
        background: {c['bg_hover']};
        border-radius: 5px;
        min-width: 24px;
    }}
    QScrollBar::handle:horizontal:hover {{
        background: {c['bg_selected']};
    }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
        width: 0;
    }}
    QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{
        background: none;
    }}

    /* ---------- QMenu (меню гамбургера, контекстные меню) ---------- */
    QMenu {{
        background-color: {c['bg_secondary']};
        border: 1px solid {c['divider']};
        padding: 4px;
    }}
    QMenu::item {{
        padding: 8px 24px 8px 12px;
        border-radius: 4px;
    }}
    QMenu::item:selected {{
        background-color: {c['bg_selected']};
    }}
    QMenu::separator {{
        height: 1px;
        background: {c['divider']};
        margin: 4px 8px;
    }}

    /* ---------- Тултипы ---------- */
    QToolTip {{
        background-color: {c['bg_hover']};
        color: {c['text_primary']};
        border: 1px solid {c['divider']};
        padding: 4px 8px;
    }}
    """


# ======================================================================
# 2. ЛЕВАЯ ПАНЕЛЬ — СПИСОК ЧАТОВ (было ui/sidebar.py)
# ======================================================================

# Роли данных в модели списка чатов (поверх стандартных Qt.DisplayRole и т.п.)
ROLE_JID = Qt.UserRole + 1
ROLE_PREVIEW = Qt.UserRole + 2
ROLE_TIME = Qt.UserRole + 3
ROLE_UNREAD = Qt.UserRole + 4

ROW_HEIGHT = 64
AVATAR_SIZE = 48
AVATAR_MARGIN = 8

# Палитра инициалов-аватаров (детерминированная по хешу JID, как в Telegram)
_AVATAR_COLORS = [
    "#e17076", "#7bc862", "#e5ca77", "#65aadd",
    "#a695e7", "#ee7aae", "#6ec9cb", "#faa774",
]


def avatar_color(jid: str) -> QColor:
    """Стабильный цвет фона аватара по JID — один и тот же контакт всегда одного цвета."""
    idx = int(hashlib.md5(jid.encode("utf-8")).hexdigest(), 16) % len(_AVATAR_COLORS)
    return QColor(_AVATAR_COLORS[idx])


def initials(jid: str) -> str:
    """Одна-две буквы для аватара: из части до '@', либо первая буква JID."""
    local = jid.split("@", 1)[0]
    parts = [p for p in local.replace(".", " ").replace("_", " ").split(" ") if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[1][0]).upper()


class ChatListDelegate(QStyledItemDelegate):
    """Рисует строку списка чатов: аватар-кружок, имя, превью, время, бейдж непрочитанных."""

    def sizeHint(self, option: QStyleOptionViewItem, index: QModelIndex) -> QSize:
        return QSize(option.rect.width(), ROW_HEIGHT)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        rect: QRect = option.rect

        if option.state & 0x0001:  # QStyle.State_Selected
            painter.fillRect(rect, QColor(TELEGRAM_DARK["bg_selected"]))
        elif option.state & 0x0040:  # QStyle.State_MouseOver
            painter.fillRect(rect, QColor(TELEGRAM_DARK["bg_hover"]))

        jid = index.data(ROLE_JID) or ""
        name = index.data(Qt.DisplayRole) or jid
        preview = index.data(ROLE_PREVIEW) or ""
        time_text = index.data(ROLE_TIME) or ""
        unread = index.data(ROLE_UNREAD) or 0

        # --- аватар ---
        ax = rect.left() + AVATAR_MARGIN
        ay = rect.top() + (rect.height() - AVATAR_SIZE) // 2
        avatar_rect = QRect(ax, ay, AVATAR_SIZE, AVATAR_SIZE)

        path = QPainterPath()
        path.addEllipse(avatar_rect.x(), avatar_rect.y(), AVATAR_SIZE, AVATAR_SIZE)
        painter.fillPath(path, avatar_color(jid))

        painter.setPen(QColor("#ffffff"))
        avatar_font = QFont(painter.font())
        avatar_font.setPointSize(13)
        avatar_font.setBold(True)
        painter.setFont(avatar_font)
        painter.drawText(avatar_rect, Qt.AlignCenter, initials(jid))

        # --- текстовая колонка ---
        text_left = avatar_rect.right() + AVATAR_MARGIN + 4
        text_right = rect.right() - AVATAR_MARGIN
        badge_reserved = 28 if unread else 0

        name_font = QFont(painter.font())
        name_font.setPointSize(10)
        name_font.setBold(True)
        painter.setFont(name_font)
        painter.setPen(QColor(TELEGRAM_DARK["text_primary"]))
        name_rect = QRect(text_left, rect.top() + 10, text_right - text_left - 60, 20)
        fm_name = QFontMetrics(name_font)
        painter.drawText(name_rect, Qt.AlignLeft | Qt.AlignVCenter, fm_name.elidedText(name, Qt.ElideRight, name_rect.width()))

        time_font = QFont(painter.font())
        time_font.setPointSize(8)
        painter.setFont(time_font)
        painter.setPen(QColor(TELEGRAM_DARK["text_secondary"]))
        time_rect = QRect(text_right - 56, rect.top() + 10, 56, 20)
        painter.drawText(time_rect, Qt.AlignRight | Qt.AlignVCenter, time_text)

        preview_font = QFont(painter.font())
        preview_font.setPointSize(9)
        painter.setFont(preview_font)
        painter.setPen(QColor(TELEGRAM_DARK["text_secondary"]))
        preview_rect = QRect(text_left, rect.top() + 34, text_right - text_left - badge_reserved, 20)
        fm_preview = QFontMetrics(preview_font)
        painter.drawText(
            preview_rect, Qt.AlignLeft | Qt.AlignVCenter,
            fm_preview.elidedText(preview, Qt.ElideRight, preview_rect.width()),
        )

        # --- бейдж непрочитанных ---
        if unread:
            badge_d = 20
            bx = rect.right() - AVATAR_MARGIN - badge_d
            by = rect.top() + 36
            badge_rect = QRect(bx, by, badge_d, badge_d)
            badge_path = QPainterPath()
            badge_path.addEllipse(bx, by, badge_d, badge_d)
            painter.fillPath(badge_path, QColor(TELEGRAM_DARK["accent"]))
            painter.setPen(QColor("#ffffff"))
            badge_font = QFont(painter.font())
            badge_font.setPointSize(7)
            badge_font.setBold(True)
            painter.setFont(badge_font)
            text = str(unread) if unread < 100 else "99+"
            painter.drawText(badge_rect, Qt.AlignCenter, text)

        painter.restore()


class SidebarWidget(QWidget):
    """Левая панель: гамбургер-меню / поиск / новый чат сверху + список диалогов.

    Не хранит self.chats само по себе — MainWindow вызывает set_chats() при
    каждом refresh_chats_list(). Это дёшево (список диалогов редко превышает
    сотни строк), зато не дублирует источник истины.
    """

    chat_selected = pyqtSignal(str)          # jid выбранного контакта
    new_chat_requested = pyqtSignal()
    menu_requested = pyqtSignal()
    search_text_changed = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("Sidebar")
        self.setFixedWidth(320)

        self._model = QStandardItemModel(self)
        self._all_jids: list[str] = []  # порядок для поиска/фильтрации

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        top = QWidget(self)
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(8, 8, 8, 8)
        top_layout.setSpacing(6)

        self.menu_btn = QToolButton(top)
        self.menu_btn.setObjectName("SidebarMenuButton")
        self.menu_btn.setText("\u2630")  # ☰
        self.menu_btn.setProperty("flat", "true")
        self.menu_btn.setToolTip("Меню")
        self.menu_btn.clicked.connect(self.menu_requested.emit)

        self.search_edit = QLineEdit(top)
        self.search_edit.setObjectName("SidebarSearch")
        self.search_edit.setPlaceholderText("\U0001F50D Поиск")
        self.search_edit.textChanged.connect(self._on_search_changed)

        self.new_chat_btn = QToolButton(top)
        self.new_chat_btn.setObjectName("SidebarNewChatButton")
        self.new_chat_btn.setText("\u270F\uFE0F")  # ✏️
        self.new_chat_btn.setProperty("flat", "true")
        self.new_chat_btn.setToolTip("Новый чат")
        self.new_chat_btn.clicked.connect(self.new_chat_requested.emit)

        top_layout.addWidget(self.menu_btn)
        top_layout.addWidget(self.search_edit, 1)
        top_layout.addWidget(self.new_chat_btn)
        root.addWidget(top)

        self.list_view = QListView(self)
        self.list_view.setObjectName("ChatListView")
        self.list_view.setModel(self._model)
        self.list_view.setItemDelegate(ChatListDelegate(self.list_view))
        self.list_view.setUniformItemSizes(True)
        self.list_view.setSelectionMode(QAbstractItemView.SingleSelection)
        self.list_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.list_view.setMouseTracking(True)  # чтобы работал State_MouseOver в делегате
        self.list_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.list_view.clicked.connect(self._on_item_clicked)
        root.addWidget(self.list_view, 1)

    # ------------------------------------------------------------------ API

    def set_chats(self, rows: list[dict]) -> None:
        """rows: [{'jid', 'name', 'preview', 'time', 'unread'}], уже отсортированные
        так, как их нужно показать (сортировку по времени делает вызывающая сторона —
        у неё есть полные данные chats, а не только эта проекция)."""
        query = self.search_edit.text().strip().lower()
        self._model.clear()
        self._all_jids = [r["jid"] for r in rows]
        for row in rows:
            if query and query not in row["jid"].lower() and query not in row.get("name", "").lower():
                continue
            item = QStandardItem(row.get("name") or row["jid"])
            item.setData(row["jid"], ROLE_JID)
            item.setData(row.get("preview", ""), ROLE_PREVIEW)
            item.setData(row.get("time", ""), ROLE_TIME)
            item.setData(row.get("unread", 0), ROLE_UNREAD)
            item.setEditable(False)
            self._model.appendRow(item)

    def select_jid(self, jid: str) -> None:
        for row in range(self._model.rowCount()):
            if self._model.item(row).data(ROLE_JID) == jid:
                self.list_view.setCurrentIndex(self._model.index(row, 0))
                return

    # ------------------------------------------------------------------ внутреннее

    def _on_item_clicked(self, index: QModelIndex) -> None:
        jid = index.data(ROLE_JID)
        if jid:
            self.chat_selected.emit(jid)

    def _on_search_changed(self, text: str) -> None:
        self.search_text_changed.emit(text)


# ======================================================================
# 3. ПРАВАЯ ПАНЕЛЬ — ПЕРЕПИСКА (было ui/chat_view.py)
# ======================================================================

ROLE_MSG = Qt.UserRole + 1

BUBBLE_MAX_WIDTH_RATIO = 0.65
BUBBLE_PAD_V = 8
BUBBLE_PAD_H = 12
BUBBLE_RADIUS = 12
GROUP_SPACING = 2       # между сообщениями одного отправителя подряд
MSG_SPACING = 8         # между сообщениями разных отправителей
TAIL_RESERVE = 46       # место под время + галочку в правом нижнем углу пузыря

_STATUS_ICON = {
    "sending": "\u23F3",      # ⏳
    "delivered": "\u2713",    # ✓
    "read": "\u2713\u2713",   # ✓✓
    "failed": "\u2717",       # ✗
}


class MessageModel(QAbstractListModel):
    """Список сообщений одного открытого диалога."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._items: list[dict] = []

    # ---- Qt.AbstractListModel API ----

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._items)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._items)):
            return None
        if role in (Qt.DisplayRole, ROLE_MSG):
            return self._items[index.row()]
        return None

    # ---- наш API ----

    def set_messages(self, messages: list[dict]) -> None:
        """Полная замена содержимого (вызывается при переключении чата)."""
        self.beginResetModel()
        self._items = list(messages)
        self.endResetModel()

    def append_message(self, msg: dict) -> None:
        """Добавить одно сообщение в конец без пересборки всей модели."""
        row = len(self._items)
        self.beginInsertRows(QModelIndex(), row, row)
        self._items.append(msg)
        self.endInsertRows()

    def update_message(self, msg_id: str, **changes) -> bool:
        """Точечно обновить сообщение по msg_id (например, статус доставки)."""
        for i, m in enumerate(self._items):
            if m.get("msg_id") == msg_id:
                m.update(changes)
                idx = self.index(i, 0)
                self.dataChanged.emit(idx, idx)
                return True
        return False

    def message_at(self, row: int) -> dict | None:
        return self._items[row] if 0 <= row < len(self._items) else None

    def is_grouped_with_previous(self, row: int) -> bool:
        """True, если сообщение row — продолжение группы того же отправителя (меньший отступ)."""
        if row <= 0:
            return False
        return self._items[row]["type"] == self._items[row - 1]["type"]


class MessageDelegate(QStyledItemDelegate):
    """Рисует один пузырь сообщения через QTextLayout (без HTML)."""

    def _bubble_font(self) -> QFont:
        f = QFont()
        f.setPointSize(9)
        return f

    def _meta_font(self) -> QFont:
        f = QFont()
        f.setPointSize(8)
        return f

    def _max_bubble_width(self, option: QStyleOptionViewItem) -> int:
        return max(120, int(option.rect.width() * BUBBLE_MAX_WIDTH_RATIO))

    def _layout_text(self, text: str, font: QFont, max_width: int) -> tuple[QTextLayout, float]:
        """Раскладывает текст с переносом по словам, возвращает layout и высоту в px."""
        layout = QTextLayout(text, font)
        text_option = QTextOption()
        text_option.setWrapMode(QTextOption.WrapAtWordBoundaryOrAnywhere)
        layout.setTextOption(text_option)

        layout.beginLayout()
        height = 0.0
        while True:
            line = layout.createLine()
            if not line.isValid():
                break
            line.setLineWidth(max_width)
            line.setPosition(QRectF(0, height, max_width, 0).topLeft())
            height += line.height()
        layout.endLayout()
        return layout, height

    def sizeHint(self, option: QStyleOptionViewItem, index: QModelIndex) -> QSize:
        msg = index.data(ROLE_MSG)
        if not msg:
            return QSize(option.rect.width(), 0)

        grouped = index.model().is_grouped_with_previous(index.row())
        max_w = self._max_bubble_width(option)
        text_w = max_w - 2 * BUBBLE_PAD_H
        _, text_h = self._layout_text(msg["message"], self._bubble_font(), text_w)

        bubble_h = int(text_h) + 2 * BUBBLE_PAD_V + 14  # +14 под строку времени/галочки
        top_margin = GROUP_SPACING if grouped else MSG_SPACING
        return QSize(option.rect.width(), bubble_h + top_margin)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:

        msg = index.data(ROLE_MSG)
        if not msg:
            return

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        rect = option.rect
        grouped = index.model().is_grouped_with_previous(index.row())
        top_margin = GROUP_SPACING if grouped else MSG_SPACING

        max_w = self._max_bubble_width(option)
        text_w = max_w - 2 * BUBBLE_PAD_H
        font = self._bubble_font()
        layout, text_h = self._layout_text(msg["message"], font, text_w)

        bubble_w = min(max_w, max(int(layout.maximumWidth()) + 2 * BUBBLE_PAD_H, 60))
        bubble_h = int(text_h) + 2 * BUBBLE_PAD_V + 14

        is_sent = msg["type"] == "sent"
        if is_sent:
            bx = rect.right() - bubble_w - 12
            bubble_color = QColor(TELEGRAM_DARK["bubble_sent"])
        else:
            bx = rect.left() + 12
            bubble_color = QColor(TELEGRAM_DARK["bubble_recv"])
        by = rect.top() + top_margin

        path = QPainterPath()
        path.addRoundedRect(bx, by, bubble_w, bubble_h, BUBBLE_RADIUS, BUBBLE_RADIUS)
        painter.fillPath(path, bubble_color)

        # --- текст сообщения ---
        painter.setPen(QColor(TELEGRAM_DARK["text_primary"]))
        painter.setFont(font)
        layout.draw(painter, QPointF(bx + BUBBLE_PAD_H, by + BUBBLE_PAD_V))

        # --- время (+ статус для исходящих) ---
        meta_font = self._meta_font()
        painter.setFont(meta_font)
        fm = QFontMetrics(meta_font)
        time_text = msg.get("time", "")

        if is_sent:
            status = msg.get("status", "sending")
            icon = _STATUS_ICON.get(status, "")
            icon_color = {
                "read": TELEGRAM_DARK["accent"],
                "delivered": TELEGRAM_DARK["text_secondary"],
                "failed": TELEGRAM_DARK["error"],
                "sending": TELEGRAM_DARK["text_secondary"],
            }.get(status, TELEGRAM_DARK["text_secondary"])

            meta_y = by + bubble_h - BUBBLE_PAD_V - fm.height() + fm.ascent()
            icon_w = fm.horizontalAdvance(icon)
            time_w = fm.horizontalAdvance(time_text)

            painter.setPen(QColor(TELEGRAM_DARK["text_secondary"]))
            painter.drawText(int(bx + bubble_w - BUBBLE_PAD_H - icon_w - time_w - 4), int(meta_y), time_text)
            painter.setPen(QColor(icon_color))
            painter.drawText(int(bx + bubble_w - BUBBLE_PAD_H - icon_w), int(meta_y), icon)
        else:
            meta_y = by + bubble_h - BUBBLE_PAD_V - fm.height() + fm.ascent()
            painter.setPen(QColor(TELEGRAM_DARK["text_secondary"]))
            painter.drawText(int(bx + bubble_w - BUBBLE_PAD_H - fm.horizontalAdvance(time_text)), int(meta_y), time_text)

        painter.restore()


class ChatViewWidget(QWidget):
    """Правая панель: шапка чата + список сообщений + поле ввода.

    Не отправляет сообщения сама — эмитит send_requested(text), а
    MainWindow.send_chat_message() (защищённое имя, логика не меняется)
    решает, что с этим текстом делать. Аналогично header_clicked/back_clicked
    только сигнализируют наверх.
    """

    send_requested = pyqtSignal(str)
    header_clicked = pyqtSignal()
    back_clicked = pyqtSignal()
    menu_action_requested = pyqtSignal(str)  # "delete_chat" и т.п.

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("ChatView")
        self._current_jid: str | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ---------- шапка ----------
        header = QWidget(self)
        header.setObjectName("ChatHeader")
        header.setFixedHeight(56)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(8, 6, 12, 6)
        header_layout.setSpacing(10)

        self.back_btn = QToolButton(header)
        self.back_btn.setObjectName("ChatBackButton")
        self.back_btn.setText("\u2190")  # ←
        self.back_btn.setProperty("flat", "true")
        self.back_btn.setVisible(False)  # показывается только в узком (мобильном) режиме
        self.back_btn.clicked.connect(self.back_clicked.emit)

        header_click_area = QWidget(header)
        hca_layout = QHBoxLayout(header_click_area)
        hca_layout.setContentsMargins(0, 0, 0, 0)
        hca_layout.setSpacing(10)

        self.avatar_label = QLabel(header_click_area)
        self.avatar_label.setObjectName("ChatHeaderAvatar")
        self.avatar_label.setFixedSize(40, 40)
        self.avatar_label.setAlignment(Qt.AlignCenter)

        name_box = QVBoxLayout()
        name_box.setSpacing(0)
        self.name_label = QLabel("Выберите диалог", header_click_area)
        self.name_label.setObjectName("ChatHeaderName")
        self.status_label = QLabel("", header_click_area)
        self.status_label.setObjectName("ChatHeaderStatus")
        name_box.addWidget(self.name_label)
        name_box.addWidget(self.status_label)

        hca_layout.addWidget(self.avatar_label)
        hca_layout.addLayout(name_box)
        hca_layout.addStretch(1)
        header_click_area.mousePressEvent = lambda _e: self.header_clicked.emit()  # noqa: E731

        self.call_btn = QToolButton(header)
        self.call_btn.setObjectName("ChatCallButton")
        self.call_btn.setText("\U0001F4DE")  # 📞 (заглушка — звонков в XMPP-клиенте нет)
        self.call_btn.setProperty("flat", "true")
        self.call_btn.setEnabled(False)
        self.call_btn.setToolTip("Звонки не поддерживаются")

        self.menu_btn = QToolButton(header)
        self.menu_btn.setObjectName("ChatMenuButton")
        self.menu_btn.setText("\u22EE")  # ⋮
        self.menu_btn.setProperty("flat", "true")
        self.menu_btn.clicked.connect(lambda: self.menu_action_requested.emit("open_menu"))

        header_layout.addWidget(self.back_btn)
        header_layout.addWidget(header_click_area, 1)
        header_layout.addWidget(self.call_btn)
        header_layout.addWidget(self.menu_btn)
        root.addWidget(header)

        # ---------- список сообщений ----------
        self.model = MessageModel(self)
        self.message_list = QListView(self)
        self.message_list.setObjectName("MessageListView")
        self.message_list.setModel(self.model)
        self.message_list.setItemDelegate(MessageDelegate(self.message_list))
        self.message_list.setVerticalScrollMode(QListView.ScrollPerPixel)
        self.message_list.setSelectionMode(QListView.NoSelection)
        self.message_list.setEditTriggers(QListView.NoEditTriggers)
        self.message_list.setResizeMode(QListView.Adjust)
        root.addWidget(self.message_list, 1)

        # ---------- поле ввода ----------
        input_bar = QWidget(self)
        input_bar.setObjectName("InputBar")
        input_layout = QHBoxLayout(input_bar)
        input_layout.setContentsMargins(10, 8, 10, 8)
        input_layout.setSpacing(8)

        self.attach_btn = QToolButton(input_bar)
        self.attach_btn.setObjectName("InputAttachButton")
        self.attach_btn.setText("\U0001F4CE")  # 📎 (заглушка — вложений в протоколе нет)
        self.attach_btn.setProperty("flat", "true")
        self.attach_btn.setEnabled(False)
        self.attach_btn.setToolTip("Вложения не поддерживаются")

        self.message_input = QTextEdit(input_bar)
        self.message_input.setObjectName("MessageInput")
        self.message_input.setPlaceholderText("Сообщение...")
        self.message_input.setFixedHeight(36)
        self.message_input.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.message_input.document().documentLayout().documentSizeChanged.connect(self._resize_input)
        self.message_input.installEventFilter(self)

        self.emoji_btn = QToolButton(input_bar)
        self.emoji_btn.setObjectName("InputEmojiButton")
        self.emoji_btn.setText("\U0001F60A")  # 😊 (заглушка — без пикера эмодзи)
        self.emoji_btn.setProperty("flat", "true")
        self.emoji_btn.setEnabled(False)
        self.emoji_btn.setToolTip("Выбор эмодзи не реализован")

        self.send_btn = QPushButton("\u27A4", input_bar)  # ➤
        self.send_btn.setObjectName("SendButton")
        self.send_btn.setFixedSize(36, 36)
        self.send_btn.setEnabled(False)
        self.send_btn.clicked.connect(self._emit_send)

        input_layout.addWidget(self.attach_btn)
        input_layout.addWidget(self.message_input, 1)
        input_layout.addWidget(self.emoji_btn)
        input_layout.addWidget(self.send_btn)
        root.addWidget(input_bar)

        self.set_enabled_for_chat(False)

    # ------------------------------------------------------------------ API для MainWindow

    def set_header(self, jid: str, status_text: str = "") -> None:
        self._current_jid = jid
        self.name_label.setText(jid)
        self.status_label.setText(status_text)

        pix_size = self.avatar_label.width()
        color = avatar_color(jid)
        # Рисуем кружок-аватар прямо в QLabel через QPixmap, чтобы не заводить
        # отдельный виджет ради одной круглой заглушки.
        from PyQt5.QtGui import QPixmap
        pm = QPixmap(pix_size, pix_size)
        pm.fill(Qt.transparent)
        p = QPainter(pm)
        p.setRenderHint(QPainter.Antialiasing)
        path = QPainterPath()
        path.addEllipse(0, 0, pix_size, pix_size)
        p.fillPath(path, color)
        p.setPen(QColor("#ffffff"))
        f = QFont(p.font())
        f.setBold(True)
        f.setPointSize(11)
        p.setFont(f)
        p.drawText(pm.rect(), Qt.AlignCenter, initials(jid))
        p.end()
        self.avatar_label.setPixmap(pm)

    def clear_header(self) -> None:
        self._current_jid = None
        self.name_label.setText("Выберите диалог")
        self.status_label.setText("")
        self.avatar_label.clear()

    def set_messages(self, messages: list[dict]) -> None:
        self.model.set_messages(messages)
        self.scroll_to_bottom()

    def append_message(self, msg: dict) -> None:
        self.model.append_message(msg)
        self.scroll_to_bottom()

    def update_message_status_visual(self, msg_id: str, **changes) -> None:
        self.model.update_message(msg_id, **changes)

    def scroll_to_bottom(self) -> None:
        QTimer.singleShot(0, lambda: self.message_list.scrollToBottom())

    def set_enabled_for_chat(self, enabled: bool) -> None:
        self.send_btn.setEnabled(enabled)
        self.message_input.setEnabled(enabled)

    def current_jid(self) -> str | None:
        return self._current_jid

    def clear_input(self) -> None:
        self.message_input.clear()

    def focus_input(self) -> None:
        self.message_input.setFocus()

    # ------------------------------------------------------------------ внутреннее

    def _resize_input(self) -> None:
        doc_h = self.message_input.document().size().height()
        new_h = max(36, min(120, int(doc_h) + 12))
        self.message_input.setFixedHeight(new_h)

    def _emit_send(self) -> None:
        text = self.message_input.toPlainText().strip()
        if text:
            self.send_requested.emit(text)

    def eventFilter(self, obj, event):  # noqa: N802 (имя метода задано Qt)
        if obj is self.message_input and event.type() == event.KeyPress:
            if event.key() in (Qt.Key_Return, Qt.Key_Enter) and not (event.modifiers() & Qt.ShiftModifier):
                self._emit_send()
                return True
        return super().eventFilter(obj, event)


# ======================================================================
# 4. ПАНЕЛЬ ПРОФИЛЯ КОНТАКТА (было ui/profile_panel.py)
# ======================================================================

class ProfilePanel(QWidget):
    """Скрыта по умолчанию (width=0). show_for(jid) выезжает слева направо."""

    delete_chat_requested = pyqtSignal()
    closed = pyqtSignal()

    PANEL_WIDTH = 320

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("ProfilePanel")
        self.setFixedWidth(0)  # свёрнута; анимация раскрывает до PANEL_WIDTH

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        close_row = QPushButton("\u2715  Закрыть", self)  # ✕
        close_row.setProperty("flat", "true")
        close_row.setStyleSheet("text-align: left; padding: 12px 16px;")
        close_row.clicked.connect(self.hide_panel)
        root.addWidget(close_row)

        self.avatar_label = QLabel(self)
        self.avatar_label.setObjectName("ProfileAvatar")
        self.avatar_label.setFixedSize(96, 96)
        self.avatar_label.setAlignment(Qt.AlignCenter)
        avatar_row = QVBoxLayout()
        avatar_row.setAlignment(Qt.AlignHCenter)
        avatar_row.addWidget(self.avatar_label, 0, Qt.AlignHCenter)
        root.addLayout(avatar_row)

        self.jid_label = QLabel("", self)
        self.jid_label.setObjectName("ProfileJid")
        self.jid_label.setAlignment(Qt.AlignCenter)
        root.addWidget(self.jid_label)

        self.status_label = QLabel("", self)
        self.status_label.setObjectName("ProfileStatus")
        self.status_label.setAlignment(Qt.AlignCenter)
        root.addWidget(self.status_label)

        root.addSpacing(16)

        self.info_list = QListWidget(self)
        self.info_list.setFrameShape(QListWidget.NoFrame)
        for text in ("\U0001F514 Уведомления — скоро", "\U0001F5BC Медиа — скоро", "\U0001F517 Ссылки — скоро"):
            item = QListWidgetItem(text)
            item.setFlags(Qt.NoItemFlags)  # не кликабельно: честно "скоро", а не имитация
            self.info_list.addItem(item)
        root.addWidget(self.info_list, 1)

        self.delete_btn = QPushButton("\U0001F5D1\uFE0F  Удалить диалог", self)  # 🗑️
        self.delete_btn.setObjectName("ProfileDeleteButton")
        self.delete_btn.clicked.connect(self.delete_chat_requested.emit)
        root.addWidget(self.delete_btn)

        self._anim = QPropertyAnimation(self, b"minimumWidth", self)
        self._anim.setDuration(180)
        self._anim.setEasingCurve(QEasingCurve.OutCubic)
        self._anim2 = QPropertyAnimation(self, b"maximumWidth", self)
        self._anim2.setDuration(180)
        self._anim2.setEasingCurve(QEasingCurve.OutCubic)

    def show_for(self, jid: str, status_text: str = "") -> None:

        self.jid_label.setText(jid)
        self.status_label.setText(status_text)

        size = self.avatar_label.width()
        pm = QPixmap(size, size)
        pm.fill(Qt.transparent)
        p = QPainter(pm)
        p.setRenderHint(QPainter.Antialiasing)
        path = QPainterPath()
        path.addEllipse(0, 0, size, size)
        p.fillPath(path, avatar_color(jid))
        p.setPen(QColor("#ffffff"))
        f = QFont(p.font())
        f.setBold(True)
        f.setPointSize(24)
        p.setFont(f)
        p.drawText(pm.rect(), Qt.AlignCenter, initials(jid))
        p.end()
        self.avatar_label.setPixmap(pm)

        self._animate_to(self.PANEL_WIDTH)

    def hide_panel(self) -> None:
        self._animate_to(0)
        self.closed.emit()

    def _animate_to(self, width: int) -> None:
        for anim in (self._anim, self._anim2):
            anim.stop()
            anim.setStartValue(self.width())
            anim.setEndValue(width)
        self._anim.start()
        self._anim2.start()


# ======================================================================
# 5. ДИАЛОГ НАСТРОЕК (было ui/dialogs/settings.py)
# ======================================================================

SECTIONS = ["Соединение", "REST API", "Уведомления", "Внешний вид"]


class SettingsDialog(QDialog):
    def __init__(self, config: dict, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("SettingsDialog")
        self.setWindowTitle("Настройки")
        self.resize(720, 520)
        self._config = config

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        body = QHBoxLayout()
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(0)

        self.nav = QListWidget(self)
        self.nav.setObjectName("SettingsNav")
        self.nav.setFixedWidth(180)
        for name in SECTIONS:
            QListWidgetItem(name, self.nav)
        self.nav.currentRowChanged.connect(self._on_nav_changed)

        self.stack = QStackedWidget(self)
        self.stack.setObjectName("SettingsStack")

        self.stack.addWidget(self._build_connection_page())
        self.stack.addWidget(self._build_rest_page())
        self.stack.addWidget(self._build_notifications_page())
        self.stack.addWidget(self._build_appearance_page())

        body.addWidget(self.nav)
        body.addWidget(self.stack, 1)
        root.addLayout(body, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Cancel | QDialogButtonBox.Save, self)
        buttons.button(QDialogButtonBox.Save).setProperty("class", "primary")
        buttons.button(QDialogButtonBox.Cancel).setProperty("class", "secondary")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)

        self.nav.setCurrentRow(0)

    # ------------------------------------------------------------------ страницы

    def _scrollable(self, inner: QWidget) -> QScrollArea:
        area = QScrollArea(self)
        area.setWidgetResizable(True)
        area.setFrameShape(QScrollArea.NoFrame)
        area.setWidget(inner)
        return area

    def _build_connection_page(self) -> QWidget:
        page = QWidget(self)
        layout = QFormLayout(page)

        xmpp = self._config.get("xmpp", {})
        self.xmpp_server = QLineEdit(xmpp.get("server", ""))
        self.xmpp_port = QSpinBox()
        self.xmpp_port.setRange(1, 65535)
        self.xmpp_port.setValue(xmpp.get("port", 5222))
        self.xmpp_username = QLineEdit(xmpp.get("username", ""))
        self.xmpp_password = QLineEdit(xmpp.get("password", ""))
        self.xmpp_password.setEchoMode(QLineEdit.Password)
        self.verify_tls = QCheckBox("Проверять TLS-сертификат")
        self.verify_tls.setChecked(xmpp.get("verify_tls", False))

        layout.addRow("\U0001F310 Сервер:", self.xmpp_server)
        layout.addRow("\U0001F50C Порт:", self.xmpp_port)
        layout.addRow("\U0001F464 Логин:", self.xmpp_username)
        layout.addRow("\U0001F511 Пароль:", self.xmpp_password)
        layout.addRow(self.verify_tls)

        return self._scrollable(page)

    def _build_rest_page(self) -> QWidget:
        page = QWidget(self)
        layout = QFormLayout(page)

        rest = self._config.get("rest_api", {})
        self.rest_host = QLineEdit(rest.get("host", ""))
        self.rest_port = QSpinBox()
        self.rest_port.setRange(1, 65535)
        self.rest_port.setValue(rest.get("port", 8080))
        self.rest_endpoint = QLineEdit(rest.get("endpoint", ""))
        self.rest_api_key = QLineEdit(rest.get("api_key", ""))
        self.rest_api_key.setEchoMode(QLineEdit.Password)
        self.rest_api_key.setPlaceholderText("Пусто = без авторизации (не рекомендуется для 0.0.0.0)")

        layout.addRow("\U0001F3E0 Хост:", self.rest_host)
        layout.addRow("\U0001F50C Порт:", self.rest_port)
        layout.addRow("\U0001F4E1 Endpoint:", self.rest_endpoint)
        layout.addRow("\U0001F510 API ключ:", self.rest_api_key)

        return self._scrollable(page)

    def _build_notifications_page(self) -> QWidget:
        # В текущем проекте нет отдельных настроек уведомлений (звук/пуши) —
        # честно показываем заглушку вместо несуществующих переключателей.
        page = QWidget(self)
        layout = QVBoxLayout(page)
        note = QLabel(
            "Настройки уведомлений пока не реализованы в десктоп-клиенте.\n"
            "Приоритет и звук уведомлений на Android-клиенте настраиваются отдельно.",
            page,
        )
        note.setWordWrap(True)
        note.setAlignment(Qt.AlignTop)
        layout.addWidget(note)
        layout.addStretch(1)
        return self._scrollable(page)

    def _build_appearance_page(self) -> QWidget:
        # Тема сейчас одна (Telegram Dark), переключателя тем нет — тоже честная заглушка.
        page = QWidget(self)
        layout = QVBoxLayout(page)
        note = QLabel("Сейчас доступна только тёмная тема (Telegram Dark).", page)
        note.setWordWrap(True)
        note.setAlignment(Qt.AlignTop)
        layout.addWidget(note)
        layout.addStretch(1)
        return self._scrollable(page)

    def _on_nav_changed(self, row: int) -> None:
        if row >= 0:
            self.stack.setCurrentIndex(row)

    # ------------------------------------------------------------------ API для MainWindow

    def get_values(self) -> dict:
        """Возвращает значения полей в форме, готовой для merge в self.config."""
        return {
            "xmpp": {
                "server": self.xmpp_server.text(),
                "port": self.xmpp_port.value(),
                "username": self.xmpp_username.text(),
                "password": self.xmpp_password.text(),
                "verify_tls": self.verify_tls.isChecked(),
            },
            "rest_api": {
                "host": self.rest_host.text(),
                "port": self.rest_port.value(),
                "endpoint": self.rest_endpoint.text(),
                "api_key": self.rest_api_key.text().strip(),
            },
        }


# ======================================================================
# 6. ДИАЛОГ ИСТОРИИ СООБЩЕНИЙ (было ui/dialogs/history.py)
# ======================================================================

class HistoryDialog(QDialog):
    def __init__(self, logger, db_path: str = "logs/messages.db", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("HistoryDialog")
        self.setWindowTitle("История сообщений")
        self.resize(900, 560)
        self.logger = logger
        self.db_path = db_path

        root = QVBoxLayout(self)

        top = QHBoxLayout()
        top.addWidget(QLabel("Фильтр:", self))
        self.contact_combo = QComboBox(self)
        self.contact_combo.setMinimumWidth(250)
        self.contact_combo.addItem("Все диалоги")
        self.contact_combo.currentTextChanged.connect(self.reload)
        top.addWidget(self.contact_combo)
        top.addStretch(1)

        self.refresh_btn = QToolButton(self)
        self.refresh_btn.setText("\U0001F504")  # 🔄
        self.refresh_btn.setToolTip("Обновить")
        self.refresh_btn.clicked.connect(self.reload)
        top.addWidget(self.refresh_btn)

        self.clean_btn = QToolButton(self)
        self.clean_btn.setText("\U0001F5D1\uFE0F")  # 🗑️
        self.clean_btn.setToolTip("Очистить старые логи (>14 дней)")
        self.clean_btn.clicked.connect(self._clean_logs)
        top.addWidget(self.clean_btn)

        root.addLayout(top)

        self.table = QTableWidget(self)
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["Тип", "Отправитель", "Получатель", "Сообщение", "Время отправки", "Время доставки"]
        )
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(38)

        header = self.table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)

        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)

        root.addWidget(self.table, 1)

        self.status_label = QLabel("Готов", self)
        root.addWidget(self.status_label)

        close_btn = QPushButton("Закрыть", self)
        close_btn.setProperty("class", "secondary")
        close_btn.clicked.connect(self.close)
        root.addWidget(close_btn, 0, Qt.AlignRight)

        self.refresh_contacts(current_user="")
        self.reload()

    # ------------------------------------------------------------------ API для MainWindow

    def refresh_contacts(self, current_user: str) -> None:
        """Пересобрать список контактов в комбобоксе (была update_contacts_list)."""
        if not os.path.exists(self.db_path):
            return
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT
                    CASE
                        WHEN sender = ? THEN recipient
                        WHEN recipient = ? THEN sender
                    END as contact
                FROM messages
                WHERE sender = ? OR recipient = ?
                """,
                (current_user, current_user, current_user, current_user),
            )
            contacts = set()
            for row in cursor.fetchall():
                contact = row[0]
                if contact and contact != current_user:
                    contact = contact.split("/")[0]
                    contacts.add(contact)
            conn.close()
        except Exception:
            return

        current = self.contact_combo.currentText()
        self.contact_combo.blockSignals(True)
        self.contact_combo.clear()
        self.contact_combo.addItem("Все диалоги")
        for contact in sorted(contacts):
            self.contact_combo.addItem(contact)
        idx = self.contact_combo.findText(current)
        self.contact_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.contact_combo.blockSignals(False)

    def reload(self) -> None:
        try:
            selected = self.contact_combo.currentText()
            self.table.setRowCount(0)
            self.status_label.setText("Загрузка...")

            if not os.path.exists(self.db_path):
                self.status_label.setText("\u274C База данных не найдена")
                return

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            if selected == "Все диалоги" or not selected:
                cursor.execute(
                    """
                    SELECT type, sender, recipient, message, send_time, delivery_time, log_time
                    FROM messages ORDER BY id DESC LIMIT 500
                    """
                )
            else:
                clean_contact = selected.split("/")[0]
                cursor.execute(
                    """
                    SELECT type, sender, recipient, message, send_time, delivery_time, log_time
                    FROM messages WHERE sender LIKE ? OR recipient LIKE ?
                    ORDER BY id DESC LIMIT 500
                    """,
                    (f"%{clean_contact}%", f"%{clean_contact}%"),
                )
            rows = cursor.fetchall()
            conn.close()

            self.table.setRowCount(len(rows))
            for i, row in enumerate(rows):
                msg_type, sender, recipient, message, send_time, delivery_time = row[0], row[1], row[2], row[3], row[4], row[5]
                message = message[:150] + "..." if len(message) > 150 else message
                send_time = send_time[:16] if send_time else "-"
                delivery_time = delivery_time[:16] if delivery_time else "-"

                type_color = "#43b581" if msg_type == "SENT" else "#5288c1"
                type_icon = "\U0001F4E4" if msg_type == "SENT" else "\U0001F4E5"

                type_item = QTableWidgetItem(f"{type_icon} {msg_type}")
                type_item.setForeground(QColor(type_color))

                self.table.setItem(i, 0, type_item)
                self.table.setItem(i, 1, QTableWidgetItem(sender.split("/")[0] if sender else "-"))
                self.table.setItem(i, 2, QTableWidgetItem(recipient.split("/")[0] if recipient else "-"))
                self.table.setItem(i, 3, QTableWidgetItem(message))
                self.table.setItem(i, 4, QTableWidgetItem(send_time))
                self.table.setItem(i, 5, QTableWidgetItem(delivery_time))

            self.status_label.setText(f"\U0001F4CA Загружено {len(rows)} сообщений")
        except Exception as exc:
            self.status_label.setText(f"\u274C Ошибка: {exc}")

    # ------------------------------------------------------------------ внутреннее

    def _show_context_menu(self, position) -> None:
        menu = QMenu(self)
        copy_action = menu.addAction("\U0001F4CB Копировать текст сообщения")
        copy_action.triggered.connect(self._copy_selected)
        menu.exec_(self.table.viewport().mapToGlobal(position))

    def _copy_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0:
            return
        item = self.table.item(row, 3)
        if item:
            from PyQt5.QtWidgets import QApplication
            QApplication.clipboard().setText(item.text())
            self.status_label.setText("\u2705 Скопировано")
            QTimer.singleShot(2000, lambda: self.status_label.setText("Готов"))

    def _clean_logs(self) -> None:
        reply = QMessageBox.question(
            self, "Очистка логов",
            "Удалить все сообщения старше 14 дней?\nЭто действие нельзя отменить.",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            try:
                self.logger.clean_old_logs(14)
                self.reload()
                QMessageBox.information(self, "Успех", "Старые логи удалены")
            except Exception as exc:
                QMessageBox.critical(self, "Ошибка", f"Ошибка очистки: {exc}")


# ======================================================================
# 7. ДИАЛОГ ЛОГОВ (было ui/dialogs/logs.py)
# ======================================================================

class LogsDialog(QDialog):
    def __init__(self, export_callback, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("LogsDialog")
        self.setWindowTitle("Логи")
        self.resize(800, 500)
        self._export_callback = export_callback

        root = QVBoxLayout(self)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("\U0001F50D Фильтр:", self))
        self.filter_combo = QComboBox(self)
        self.filter_combo.addItems(["Все", "Отправленные", "Полученные", "Ошибки"])
        filter_row.addWidget(self.filter_combo)
        filter_row.addStretch(1)
        root.addLayout(filter_row)

        self.log_view = QTextEdit(self)
        self.log_view.setObjectName("LogsView")
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Consolas", 9))
        root.addWidget(self.log_view, 1)

        btn_row = QHBoxLayout()
        export_btn = QPushButton("\U0001F4C1 Экспорт JSON", self)
        export_btn.clicked.connect(self._export)
        clear_btn = QPushButton("\U0001F5D1\uFE0F Очистить", self)
        clear_btn.clicked.connect(self.log_view.clear)
        btn_row.addWidget(export_btn)
        btn_row.addWidget(clear_btn)
        btn_row.addStretch(1)
        close_btn = QPushButton("Закрыть", self)
        close_btn.setProperty("class", "secondary")
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(close_btn)
        root.addLayout(btn_row)

    # ------------------------------------------------------------------ API для MainWindow

    def load_entries(self, html_entries: list[str]) -> None:
        self.log_view.clear()
        for entry in html_entries:
            self.log_view.append(entry)
        self._scroll_to_bottom()

    def append_entry(self, html_entry: str) -> None:
        self.log_view.append(html_entry)
        self._scroll_to_bottom()

    def _scroll_to_bottom(self) -> None:
        scrollbar = self.log_view.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _export(self) -> None:
        from PyQt5.QtWidgets import QMessageBox
        try:
            filepath = self._export_callback()
            QMessageBox.information(self, "Успех", f"Логи экспортированы в:\n{filepath}")
        except Exception as exc:
            QMessageBox.critical(self, "Ошибка", f"Ошибка экспорта: {exc}")


# ======================================================================
# 8. ГЛАВНОЕ ОКНО XMPPGUI (было main_window.py)
# ======================================================================

os.environ['LANG'] = 'ru_RU.UTF-8'


class XMPPGUI(QMainWindow):
    # --- Межпоточные сигналы (объявлены на уровне класса) --- НЕ ТРОГАТЬ
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

        # Буфер логов для LogsDialog (диалог не живёт постоянно — см. docstring, п.3)
        self._log_entries: list[str] = []

        # Диалоги открываются по требованию, ссылки держим, чтобы не плодить
        # копии и чтобы можно было форвардить обновления (reload) в открытый.
        self.history_dialog: HistoryDialog | None = None
        self.logs_dialog: LogsDialog | None = None

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

    # ================================================================== UI

    def init_ui(self):
        self.setObjectName("MainWindow")
        self.setWindowTitle("XMPP Мессенджер")
        self.setGeometry(100, 100, 1100, 720)

        font = QFont("Segoe UI", 9)
        self.setFont(font)
        self.setStyleSheet(build_qss())

        splitter = QSplitter(Qt.Horizontal, self)

        self.sidebar = SidebarWidget(splitter)
        self.sidebar.chat_selected.connect(self._on_chat_selected)
        self.sidebar.new_chat_requested.connect(self.add_new_chat)
        self.sidebar.menu_requested.connect(self._show_hamburger_menu)

        right_container = QWidget(splitter)
        right_layout = QHBoxLayout(right_container)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)

        self.chat_view = ChatViewWidget(right_container)
        self.chat_view.send_requested.connect(self._on_send_requested)
        self.chat_view.header_clicked.connect(self._toggle_profile_panel)
        self.chat_view.menu_action_requested.connect(self._on_chat_menu_action)

        self.profile_panel = ProfilePanel(right_container)
        self.profile_panel.delete_chat_requested.connect(self.delete_current_chat)

        right_layout.addWidget(self.chat_view, 1)
        right_layout.addWidget(self.profile_panel)

        splitter.addWidget(self.sidebar)
        splitter.addWidget(right_container)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([320, 780])

        self.setCentralWidget(splitter)

        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.status_label = QLabel("\U0001F534 Не подключено")
        self.statusBar.addWidget(self.status_label)

        # Таймер обновления чата — реже и только при изменениях (поведение не менялось)
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
            if self.history_dialog is not None and self.history_dialog.isVisible():
                self.history_dialog.reload()

    # ------------------------------------------------------------ гамбургер-меню

    def _show_hamburger_menu(self) -> None:
        menu = QMenu(self)
        menu.addAction("\U0001F464 Мой профиль", self._show_my_profile)
        menu.addAction("\u2699\uFE0F Настройки", self.open_settings_dialog)
        menu.addAction("\U0001F4DC История", self.open_history_dialog)
        menu.addAction("\U0001F4CB Логи", self.open_logs_dialog)
        menu.addSeparator()
        menu.addAction("\U0001F50C Подключиться", self.connect_xmpp)
        menu.addAction("\U0001F464 Создать пользователя", self.open_register_dialog)
        menu.addAction("\U0001F680 Запустить REST", self.start_rest)
        menu.addSeparator()
        menu.addAction("\u2139\uFE0F О программе", self._show_about)
        menu.addAction("\U0001F6AA Выход", self.close)
        menu.exec_(self.sidebar.menu_btn.mapToGlobal(self.sidebar.menu_btn.rect().bottomLeft()))

    def _show_my_profile(self) -> None:
        username = self.config.get("xmpp", {}).get("username", "-")
        server = self.config.get("xmpp", {}).get("server", "-")
        QMessageBox.information(self, "Мой профиль", f"\U0001F464 {username}@{server}")

    def _show_about(self) -> None:
        from version import VERSION
        QMessageBox.about(
            self, "О программе",
            "<h2 style='color:#5288c1;'>\U0001F4E8 XMPP Мессенджер</h2>"
            f"<p><b>Версия:</b> {VERSION}</p>"
            "<p><b>Протокол:</b> XMPP (Jabber)</p>"
            "<p><b>API:</b> REST</p>"
            "<ul>"
            "<li>Статусы сообщений: \u2713 доставлено, \u2713\u2713 прочитано</li>"
            "<li>REST API для внешних систем</li>"
            "<li>История сообщений с фильтром по контактам</li>"
            "<li>Автоочистка логов каждые 14 дней</li>"
            "</ul>",
        )

    # ------------------------------------------------------------ диалоги по требованию

    def open_settings_dialog(self) -> None:
        dialog = SettingsDialog(self.config, self)
        if dialog.exec_() == QDialog.Accepted:
            values = dialog.get_values()
            self.config["xmpp"].update(values["xmpp"])
            self.config["rest_api"].update(values["rest_api"])
            self.persist_settings()
            self.log_callback("Настройки сохранены", "INFO")

    def open_history_dialog(self) -> None:
        if self.history_dialog is None:
            self.history_dialog = HistoryDialog(self.logger, parent=self)
        current_user = self.config.get("xmpp", {}).get("username", "")
        self.history_dialog.refresh_contacts(current_user)
        self.history_dialog.reload()
        self.history_dialog.show()
        self.history_dialog.raise_()

    def open_logs_dialog(self) -> None:
        if self.logs_dialog is None:
            self.logs_dialog = LogsDialog(self.logger.export_to_json, parent=self)
            self.logs_dialog.load_entries(self._log_entries)
        self.logs_dialog.show()
        self.logs_dialog.raise_()

    # ------------------------------------------------------------ список/чат

    def add_new_chat(self):
        contact, ok = QInputDialog.getText(
            self, "Новый чат", "Введите JID пользователя (например: username@arsenal):"
        )
        if ok and contact:
            if contact not in self.chats:
                self.chats[contact] = []
                self.save_chat_history()
                self.refresh_chats_list()
                self.log_callback(f"Начат чат с {contact}", "INFO")
                self._open_chat(contact)
            else:
                QMessageBox.warning(self, "Внимание", "Чат с этим пользователем уже существует")

    def _on_chat_selected(self, jid: str) -> None:
        self._open_chat(jid)

    def _open_chat(self, contact: str) -> None:
        self.current_contact = contact
        self.chat_view.set_header(contact)
        self.chat_view.set_enabled_for_chat(True)
        self.sidebar.select_jid(contact)
        self.mark_messages_as_read(contact)
        self.refresh_chat_display()
        self.chat_view.focus_input()

    def _toggle_profile_panel(self) -> None:
        jid = self.chat_view.current_jid()
        if not jid:
            return
        if self.profile_panel.width() > 0:
            self.profile_panel.hide_panel()
        else:
            self.profile_panel.show_for(jid)

    def _on_chat_menu_action(self, action: str) -> None:
        if action == "open_menu":
            menu = QMenu(self)
            menu.addAction("\U0001F5D1\uFE0F Удалить диалог", self.delete_current_chat)
            menu.exec_(self.chat_view.menu_btn.mapToGlobal(self.chat_view.menu_btn.rect().bottomRight()))

    def delete_current_chat(self):
        if not self.current_contact:
            QMessageBox.warning(self, "Ошибка", "Выберите диалог для удаления")
            return

        reply = QMessageBox.question(
            self, "Удаление диалога",
            f"Удалить диалог с {self.current_contact}? История переписки будет удалена.",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            contact = self.current_contact

            if contact in self.chats:
                del self.chats[contact]
                self.save_chat_history()

                conn = sqlite3.connect('logs/messages.db')
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM messages WHERE sender LIKE ? OR recipient LIKE ?",
                    (f'%{contact}%', f'%{contact}%'),
                )
                conn.commit()
                conn.close()

                self.current_contact = None
                self.chat_view.clear_header()
                self.chat_view.set_enabled_for_chat(False)
                self.profile_panel.hide_panel()
                self.refresh_chats_list()
                self.log_callback(f"Диалог с {contact} удален", "INFO")
                self._history_dirty = True

    # ---- protected: имя/поведение не менять, меняются только виджеты внутри ----

    def refresh_chats_list(self):
        entries = []
        for contact, messages in self.chats.items():
            last_msg = messages[-1]["message"][:60] if messages else ""
            last_time = messages[-1]["time"] if messages else ""
            # Время последней активности: полный timestamp (ts) у новых сообщений,
            # 0.0 — для старых диалогов без ts и пустых чатов (остаются внизу).
            last_ts = messages[-1].get("ts", 0.0) if messages else 0.0
            entries.append((
                last_ts,
                {
                    "jid": contact,
                    "name": contact,
                    "preview": last_msg,
                    "time": last_time,
                    "unread": self.get_unread_count(contact),
                },
            ))
        # Свежие диалоги — выше (сортировка стабильна: при равных ts сохраняется
        # порядок вставки в self.chats).
        entries.sort(key=lambda e: e[0], reverse=True)
        self.sidebar.set_chats([row for _, row in entries])

    def get_unread_count(self, contact):
        if contact not in self.chats:
            return 0
        return sum(1 for msg in self.chats[contact]
                   if msg.get('type') == 'received' and not msg.get('read', False))

    def refresh_chat_display(self):
        if not self.current_contact or self.current_contact not in self.chats:
            self.chat_view.set_messages([])
            return

        messages = self.chats[self.current_contact]
        self.chat_view.set_messages(messages)

        # Как и в исходной версии: открытый чат неявно помечает входящие read
        # (реальную displayed-квитанцию по XMPP шлёт mark_messages_as_read).
        changed = False
        for msg in messages:
            if msg["type"] == "received" and not msg.get("read", False):
                msg["read"] = True
                changed = True
        if changed:
            self.save_chat_history()

        self.refresh_chats_list()

    def send_chat_message(self):
        if not self.current_contact:
            QMessageBox.warning(self, "Ошибка", "Выберите чат")
            return

        if not self.xmpp_service or not self.xmpp_service.client.is_connected:
            QMessageBox.warning(self, "Ошибка", "Нет подключения к XMPP серверу")
            return

        message = self.chat_view.message_input.toPlainText().strip()
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
                'ts': time.time(),
                'status': 'sending',
                'msg_id': msg_id,
            })
            self.pending_messages[msg_id] = self.current_contact
            self.save_chat_history()
            self.refresh_chat_display()
            self.chat_view.clear_input()

            self.xmpp_service.client.queue_message(
                self.current_contact, message, msg_id=msg_id
            )
            self.log_callback(f"Сообщение поставлено в очередь: {self.current_contact}", "INFO")

        except Exception as e:
            self.update_message_status(msg_id, 'failed')
            QMessageBox.critical(self, "Ошибка", f"Не удалось отправить: {str(e)}")

    def _on_send_requested(self, _text: str) -> None:
        """ChatViewWidget сам не отправляет — просит XMPPGUI сделать это по имени
        send_chat_message (имя/поведение метода не меняется)."""
        self.send_chat_message()

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
                                f"\u2705 Отправлено подтверждение прочтения для {contact}", "INFO"
                            )
                except Exception as e:
                    self.log_callback(f"Ошибка отправки подтверждения: {str(e)}", "ERROR")

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
        self.log_callback(f"\u2713 Доставлено: {msg_id}", "INFO")
        self.update_message_status(msg_id, 'delivered')

    @pyqtSlot(str)
    def _on_receipt_received_safe(self, msg_id):
        self.log_callback(
            f'DEBUG read: msg_id={msg_id} pending={list(self.pending_messages.keys())[:5]}', 'INFO'
        )
        self.logger.mark_read(msg_id)
        self.log_callback(f"\u2713\u2713 Прочитано: {msg_id}", "INFO")
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
                'ts': time.time(),
                'read': False,
                'msg_id': msg_id,
            })
            self.save_chat_history()

            self.log_callback(f"\U0001F4E9 Получено сообщение от {from_jid}: {message}", "INFO")

            self._history_dirty = True
            self.refresh_chats_list()

            if self.current_contact == from_jid:
                self._chat_dirty = True
                if msg_id and self.xmpp_service and self.xmpp_service.client and self.xmpp_service.client.is_connected:
                    self.xmpp_service.client.send_displayed_marker(from_jid, msg_id)
            else:
                self.status_label.setText(f"\U0001F4E9 Новое сообщение от {from_jid}")
                QTimer.singleShot(3000, lambda: self.status_label.setText("\U0001F7E2 Подключено"))
        except Exception as e:
            self.log_callback(f"Ошибка обработки сообщения: {str(e)}", "ERROR")

    # ================================================================== подключение / REST

    def connect_xmpp(self):
        # Решение 1 из docstring: логин/пароль читаются из self.config,
        # а не из виджета настроек (тот теперь не живёт постоянно).
        username = self.config.get("xmpp", {}).get("username", "").strip()
        password = self.config.get("xmpp", {}).get("password", "")

        if not username or not password:
            QMessageBox.warning(self, "Ошибка", "Введите логин и пароль в настройках")
            return

        if getattr(self, "_connect_worker", None) and self._connect_worker.isRunning():
            return

        self.status_label.setText("\u23F3 Подключение...")
        self.log_callback("Подключение к XMPP...", "INFO")

        self._connect_worker = XmppConnectWorker(
            self.config, self.log_callback, username, password, self.kafka_broker
        )
        self._connect_worker.finished.connect(self._on_xmpp_connect_finished)
        self._connect_worker.start()

    def _on_xmpp_connect_finished(self, success: bool, error: str):
        username = self.config.get("xmpp", {}).get("username", "").strip()

        if success and self._connect_worker.service:
            self.xmpp_service = self._connect_worker.service
            self.status_label.setText(f"\U0001F7E2 Подключено: {username}")
            self.log_callback("Подключено к XMPP серверу", "INFO")

            self.xmpp_service.client.on_message_received = self.receive_message
            self.xmpp_service.client.on_delivery_received = self.on_delivery_received
            self.xmpp_service.client.on_read_received = self.on_receipt_received
            self.xmpp_service.client.on_send_failed = self.on_send_failed

            if self.history_dialog is not None:
                self.history_dialog.refresh_contacts(username)
        else:
            self.status_label.setText("\U0001F534 Не подключено")
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

        form_layout.addRow("\U0001F464 Логин:", username_edit)
        form_layout.addRow("\U0001F511 Пароль:", password_edit)
        form_layout.addRow("\U0001F511 Подтверждение:", confirm_edit)
        form_layout.addRow("\U0001F4E7 Email:", email_edit)
        layout.addLayout(form_layout)

        btn_layout = QHBoxLayout()
        register_btn = QPushButton("\u2705 Зарегистрировать")
        cancel_btn = QPushButton("\u274C Отмена")
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
                    QMessageBox.information(
                        dialog, "Успех",
                        f"Запрос на регистрацию пользователя {username} отправлен.\n"
                        f"Если сервер поддерживает регистрацию, пользователь будет создан.",
                    )
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

                    with self.chats_lock:
                        if to_user not in self.chats:
                            self.chats[to_user] = []
                        self.chats[to_user].append({
                            'type': 'sent',
                            'message': message,
                            'time': datetime.now().strftime("%H:%M:%S"),
                            'ts': time.time(),
                            'status': 'sending',
                            'msg_id': msg_id,
                        })
                    with self.pending_messages_lock:
                        self.pending_messages[msg_id] = to_user

                    self.save_chat_history()

                    result = self.xmpp_service.send_message(
                        to_user, message, msg_id=msg_id, priority=priority
                    )

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

            self.log_callback(f"REST API запущен на порту {self.config['rest_api']['port']}", "INFO")
            QMessageBox.information(
                self, "Успех", f"REST API запущен на порту {self.config['rest_api']['port']}"
            )

        except Exception as e:
            self.log_callback(f"Ошибка запуска REST: {str(e)}", "ERROR")
            QMessageBox.critical(self, "Ошибка", f"Ошибка: {str(e)}")

    def export_logs(self):
        try:
            filepath = self.logger.export_to_json()
            QMessageBox.information(self, "Успех", f"Логи экспортированы в:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка экспорта: {str(e)}")

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
        # Решение 1 из docstring: тоже читаем из self.config.
        username = self.config.get("xmpp", {}).get("username", "")
        password = self.config.get("xmpp", {}).get("password", "")

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
                    f"{msg_data.get('sender', '?')} \u2192 {msg_data.get('recipient', '?')}: "
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
                    if msg_data.get('send_time') else None,
                    delivery_time=datetime.fromisoformat(msg_data['delivery_time'])
                    if msg_data.get('delivery_time') else None,
                    read_time=datetime.fromisoformat(msg_data['read_time'])
                    if msg_data.get('read_time') else None,
                )
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                display_message = message

        color = "#7f91a4"
        if level == "ERROR":
            color = "#e53935"
        elif level == "MESSAGE":
            color = "#4dcd5e"

        safe_display = html.escape(display_message)
        log_entry = (
            f'<span style="color: #7f91a4;">[{timestamp}]</span> '
            f'<span style="color: {color};">[{level}]</span> {safe_display}<br>'
        )

        # --- решение 3 из docstring: log_text больше не существует постоянно,
        #     пишем в буфер и форвардим в LogsDialog, если он открыт ---
        self._log_entries.append(log_entry)
        if len(self._log_entries) > 5000:
            self._log_entries = self._log_entries[-5000:]
        if self.logs_dialog is not None and self.logs_dialog.isVisible():
            self.logs_dialog.append_entry(log_entry)

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