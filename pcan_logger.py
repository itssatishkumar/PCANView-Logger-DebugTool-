# pcan_logger.py
import sys
import time
from collections import deque
from ctypes import c_ubyte
from parse_tool import trc_to_csv, parse_log_to_compact_csv
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget, QSplitter, QStatusBar, QLabel,
    QToolBar, QPushButton, QHBoxLayout, QFileDialog, QHeaderView,
    QMenu, QDialog, QGridLayout, QLineEdit, QComboBox, QCheckBox,
    QTabWidget, QFrame, QToolButton, QWidgetAction, QMessageBox,
    QProgressDialog
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QPoint

# ----------------------------
# Import PCANBasic module
# ----------------------------
from PCANBasic import *  # noqa: F401,F403  (keep using constants directly like PCAN_USBBUS1)

# Safe fallback for missing constants
PCAN_ERROR_ILLEGAL_PARAMETER = getattr(
    sys.modules.get('PCANBasic', None), "PCAN_ERROR_ILLEGAL_PARAMETER", 0x40000
)
PCAN_ERROR_QRCVEMPTY = getattr(
    sys.modules.get('PCANBasic', None), "PCAN_ERROR_QRCVEMPTY", 0x00000003
)

import updater  # Import updater module
from filesize import LogFileHandler  # <-- ensure filesize.py is present

# ----------------------------
# CAN configuration
# ----------------------------
CAN_CHANNEL = PCAN_USBBUS1
CAN_BAUDRATE = PCAN_BAUD_250K

# ----------------------------
# UI-friendly constants / tuning
# ----------------------------
TRACE_ROW_LIMIT = 200            # keep only latest 200 rows
TRACE_FLUSH_INTERVAL_MS = 50     # flush pending messages to UI every 50 ms
TRACE_ROWS_PER_FLUSH = 25        # limit rows processed per flush to keep UI responsive

# ----------------------------
# Worker Thread for Receiving CAN Messages
# - Manages init/reconnect itself
# - Emits message_received(msg, ts_us) to GUI thread
# - Emits status_changed(bool) only after a successful initial connect to avoid false disconnects
# ----------------------------
class CANReader(QThread):
    message_received = Signal(object, object)  # (msg, ts_us)
    status_changed = Signal(bool)             # True = connected, False = disconnected
    error_occurred = Signal(str)

    def __init__(self, pcan, channel, baudrate, parent=None):
        super().__init__(parent)
        self.pcan = pcan
        self.channel = channel
        self.baudrate = baudrate
        self.running = True

        # state
        self.connected = False
        self.ever_connected = False  # important: avoid reporting disconnects before first success

    def run(self):
        # Loop: try to initialize, then read; on problems try to reconnect.
        while self.running:
            if not self.connected:
                try:
                    res = self.pcan.Initialize(self.channel, self.baudrate)
                except Exception as e:
                    self.error_occurred.emit(f"PCAN Initialize exception: {e}")
                    res = 1

                if res == PCAN_ERROR_OK:
                    self.connected = True
                    self.ever_connected = True
                    # Inform GUI we are connected
                    self.status_changed.emit(True)
                else:
                    # Not connected yet — don't spam the GUI with disconnect events.
                    time.sleep(0.8)
                    continue

            # Connected: perform read loop. Use non-blocking Read and check status.
            try:
                sts = self.pcan.GetStatus(self.channel)
            except Exception as e:
                # Treat as lost connection; force reconnect path
                self.error_occurred.emit(f"GetStatus exception: {e}")
                sts = PCAN_ERROR_ILLEGAL_PARAMETER

            if sts != PCAN_ERROR_OK:
                # Lost connection — uninitialize and report (only if we had connected before)
                try:
                    self.pcan.Uninitialize(self.channel)
                except Exception:
                    pass
                self.connected = False
                # Only emit disconnected if we had previously been connected (avoid false alarms during startup)
                if self.ever_connected:
                    self.status_changed.emit(False)
                time.sleep(0.8)
                continue

            # Try reading frames
            try:
                result, msg, timestamp = self.pcan.Read(self.channel)
            except Exception as e:
                # treat read exception as disconnect/reconnect cycle
                self.error_occurred.emit(f"PCAN Read exception: {e}")
                try:
                    self.pcan.Uninitialize(self.channel)
                except Exception:
                    pass
                if self.ever_connected:
                    self.status_changed.emit(False)
                self.connected = False
                time.sleep(0.8)
                continue

            if result == PCAN_ERROR_OK:
                # convert timestamp structure to microseconds (matches original behavior)
                ts_us = timestamp.micros + timestamp.millis * 1000
                # emit the message object and timestamp
                self.message_received.emit(msg, ts_us)
            elif result == PCAN_ERROR_QRCVEMPTY:
                # nothing to read, yield CPU briefly
                time.sleep(0.001)
            else:
                # unexpected error code — notify and check/connect in next loop
                self.error_occurred.emit(f"PCAN Read return: {hex(result)}")
                # optionally check status which will trigger reconnect
                time.sleep(0.002)

        # Thread stopping: ensure uninitialize
        try:
            if self.connected:
                self.pcan.Uninitialize(self.channel)
        except Exception:
            pass
        # If we were connected, inform GUI we are now disconnected
        if self.connected and self.ever_connected:
            self.status_changed.emit(False)
        self.connected = False

    def stop(self):
        self.running = False


# ----------------------------
# Generic Worker for file parsing (unchanged)
# ----------------------------
class WorkerThread(QThread):
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            res = self.func(*self.args, **self.kwargs)
            if isinstance(res, str) and res:
                msg = res
            else:
                msg = "Conversion completed."
            self.finished_signal.emit(msg)
        except Exception as e:
            self.error_signal.emit(str(e))


# ----------------------------
# Popup dialog for New Transmit Message (unchanged)
# ----------------------------
class NewMessageDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("New Transmit Message")
        layout = QGridLayout()
        layout.addWidget(QLabel("ID (hex):"), 0, 0)
        self.id_input = QLineEdit("000")
        layout.addWidget(self.id_input, 0, 1)
        layout.addWidget(QLabel("Length:"), 1, 0)
        self.len_combo = QComboBox()
        self.len_combo.addItems([str(i) for i in range(1, 9)])
        self.len_combo.setCurrentText("8")
        layout.addWidget(self.len_combo, 1, 1)
        layout.addWidget(QLabel("Data (hex):"), 2, 0)
        self.data_inputs = []
        data_layout = QHBoxLayout()
        for _ in range(8):
            box = QLineEdit("00")
            box.setMaxLength(2)
            box.setFixedWidth(30)
            self.data_inputs.append(box)
            data_layout.addWidget(box)
        layout.addLayout(data_layout, 2, 1)
        layout.addWidget(QLabel("Cycle Time (ms):"), 3, 0)
        self.cycle_input = QLineEdit("100")
        layout.addWidget(self.cycle_input, 3, 1)
        self.chk_extended = QCheckBox("Extended Frame")
        layout.addWidget(self.chk_extended, 4, 0)
        self.chk_remote = QCheckBox("Remote Request")
        layout.addWidget(self.chk_remote, 4, 1)
        layout.addWidget(QLabel("Comment:"), 5, 0)
        self.comment_input = QLineEdit("")
        layout.addWidget(self.comment_input, 5, 1)
        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout, 6, 0, 1, 2)
        self.setLayout(layout)

    def get_data(self):
        return {
            "id": self.id_input.text(),
            "length": int(self.len_combo.currentText()),
            "data": [box.text() for box in self.data_inputs],
            "cycle": self.cycle_input.text(),
            "extended": self.chk_extended.isChecked(),
            "remote": self.chk_remote.isChecked(),
            "comment": self.comment_input.text()
        }


# ----------------------------
# Main Window (kept UI exactly as you wanted)
# - Modified only connection & trace handling
# ----------------------------
class PCANViewClone(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PCAN-View (Logger & DebugTools)")
        self.resize(1300, 700)

        # PCAN instance
        self.pcan = PCANBasic()

        # Reader thread
        self.reader = None

        # connection / logging state
        self.is_connected = False
        self.live_data = {}
        self.log_handler = None
        self.log_start_time = None
        self.recording_start_time = None
        self.message_count = 0
        self.logging = False
        self.current_log_filename = None
        self.header_written = False

        # track connection start used for non-recording trace timestamps
        self.connection_start_time = None

        # trace buffering
        self.trace_buffer = deque()            # store full rows as lists
        self.max_trace_messages = TRACE_ROW_LIMIT

        # pending messages from reader - flushed to UI on timer to avoid UI freeze
        self._pending_trace = deque()

        # --- UI setup (kept intact) ---
        toolbar = QToolBar("Main Toolbar")
        toolbar.setStyleSheet("QToolBar { background-color: #0078D7; }")
        self.addToolBar(toolbar)

        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.toggle_connection)
        self.style_toolbar_button(self.connect_btn)
        toolbar.addWidget(self.connect_btn)

        self.log_start_btn = QPushButton("Start Logging")
        self.log_start_btn.clicked.connect(self.ask_log_filename)
        self.style_toolbar_button(self.log_start_btn, bg="green")
        toolbar.addWidget(self.log_start_btn)

        self.log_stop_btn = QPushButton("Stop Logging")
        self.log_stop_btn.clicked.connect(self.stop_logging)
        self.log_stop_btn.setEnabled(False)
        self.style_toolbar_button(self.log_stop_btn, bg="red")
        toolbar.addWidget(self.log_stop_btn)

        self.trace_btn = QPushButton("Trace")
        self.trace_btn.clicked.connect(self.switch_to_trace_tab)
        self.style_toolbar_button(self.trace_btn, bg="#444")
        toolbar.addWidget(self.trace_btn)

        # Tabs and receive/transmit/trace UI (unchanged)
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self.recv_tx_tab = QWidget()
        self.setup_recv_tx_tab()
        self.tabs.addTab(self.recv_tx_tab, "Receive / Transmit")

        self.trace_tab = QWidget()
        self.setup_trace_tab()
        self.tabs.addTab(self.trace_tab, "Trace")

        # Status bar (unchanged)
        self.status_bar = QStatusBar()
        self.status_conn = QLabel("Disconnected")
        self.status_bitrate = QLabel("Bit rate: ---")
        self.status_bus = QLabel("Status: ---")
        self.status_bar.addWidget(self.status_conn)
        self.status_bar.addWidget(self.status_bitrate)
        self.status_bar.addWidget(self.status_bus)
        self.setStatusBar(self.status_bar)

        # Parse tool button (unchanged)
        self.parse_toolbutton = QToolButton()
        self.parse_toolbutton.setText("Parse File")
        self.parse_toolbutton.setStyleSheet(
            "QToolButton { background-color: green; color: white; font-weight: bold; padding: 6px; }"
            "QToolButton:pressed { background-color: darkgreen; }"
            "QToolButton:hover { background-color: green; }"
        )
        self.parse_menu = QMenu(self.parse_toolbutton)

        def create_colored_action(text, color):
            action = QWidgetAction(self.parse_menu)
            btn = QPushButton(text)
            btn.setStyleSheet(f"""
                QPushButton {{
                    background-color: {color};
                    color: white;
                    font-weight: bold;
                    border: none;
                    padding: 6px 12px;
                    text-align: left;
                }}
                QPushButton:pressed {{
                    background-color: {color};
                }}
                QPushButton:hover {{
                    background-color: {color};
                }}
            """)
            btn.clicked.connect(lambda checked=False, t=text: self._parse_menu_action_triggered(t))
            action.setDefaultWidget(btn)
            return action

        self.parse_menu.addAction(create_colored_action("TRC → CSV", "#dc0d33"))
        self.parse_menu.addAction(create_colored_action("LOG → CSV", "#09ad3d"))
        self.parse_toolbutton.setMenu(self.parse_menu)
        self.parse_toolbutton.setPopupMode(QToolButton.InstantPopup)
        self.tabs.setCornerWidget(self.parse_toolbutton, Qt.TopRightCorner)

        self.setStyleSheet("""
            QMainWindow { background-color: #f0f0f0; }
            QTableWidget { background: white; alternate-background-color: #e6f2ff; gridline-color: #c0c0c0; }
            QHeaderView::section { background-color: #0078D7; color: white; padding: 4px; }
        """)

        # Auto-send timer (unchanged)
        self.auto_send_timer = QTimer()
        self.auto_send_timer.timeout.connect(self.auto_send_messages)
        self.auto_send_timer.start(100)

        # Worker/progress references
        self._worker_thread = None
        self._progress_dialog = None

        # Blink timer for logging status
        self._blink_timer = QTimer()
        self._blink_timer.timeout.connect(self._blink_status_text)
        self._blink_state = False

        # Timer to flush pending trace rows to UI (smoothing)
        self._flush_timer = QTimer()
        self._flush_timer.setInterval(TRACE_FLUSH_INTERVAL_MS)
        self._flush_timer.timeout.connect(self._flush_pending_trace)
        self._flush_timer.start()

    # parse menu helper
    def _parse_menu_action_triggered(self, text):
        if text == "TRC → CSV":
            self.convert_trc_to_csv()
        elif text == "LOG → CSV":
            self.convert_log_to_csv()

    # ----------------------------
    # UI setup helpers (unchanged)
    # ----------------------------
    def setup_recv_tx_tab(self):
        layout = QVBoxLayout()
        splitter = QSplitter(Qt.Vertical)

        receive_frame = QFrame()
        receive_layout = QVBoxLayout(receive_frame)
        lbl_rx = QLabel("Receive")
        lbl_rx.setStyleSheet("background:#e0e0e0; padding:2px; font-weight:bold;")
        receive_layout.addWidget(lbl_rx)

        self.receive_table = QTableWidget()
        self.receive_table.setColumnCount(4)
        self.receive_table.setHorizontalHeaderLabels(["CAN ID", "Count", "Cycle Time (ms)", "Data"])
        self.receive_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.receive_table.setAlternatingRowColors(True)
        receive_layout.addWidget(self.receive_table)

        transmit_frame = QFrame()
        transmit_layout = QVBoxLayout(transmit_frame)
        lbl_tx = QLabel("Transmit")
        lbl_tx.setStyleSheet("background:#e0e0e0; padding:2px; font-weight:bold;")
        transmit_layout.addWidget(lbl_tx)

        self.transmit_table = QTableWidget()
        self.transmit_table.setColumnCount(8)
        self.transmit_table.setHorizontalHeaderLabels(
            ["Enable", "CAN-ID", "Type", "Length", "Data", "Cycle Time(ms)", "Count", "Comment"])
        self.transmit_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.transmit_table.setAlternatingRowColors(True)
        self.transmit_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.transmit_table.customContextMenuRequested.connect(self.show_context_menu)
        transmit_layout.addWidget(self.transmit_table)

        splitter.addWidget(receive_frame)
        splitter.addWidget(transmit_frame)
        splitter.setSizes([350, 350])
        layout.addWidget(splitter)
        self.recv_tx_tab.setLayout(layout)

    def setup_trace_tab(self):
        layout = QVBoxLayout()
        self.trace_table = QTableWidget()
        self.trace_table.setColumnCount(5)
        self.trace_table.setHorizontalHeaderLabels(["Time (s)", "CAN ID", "Rx/Tx", "Length", "Data"])
        self.trace_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.trace_table.setAlternatingRowColors(True)
        self.trace_table.setRowCount(0)
        layout.addWidget(self.trace_table)
        self.trace_tab.setLayout(layout)

    # ----------------------------
    # Styling helper (unchanged)
    # ----------------------------
    def style_toolbar_button(self, button, bg="#0078D7"):
        button.setStyleSheet(f"""
            QPushButton {{ background-color: {bg}; color: white; font-weight: bold; padding: 6px; }}
            QPushButton:hover {{ background-color: #005fa3; }}
        """)

    def show_context_menu(self, pos: QPoint):
        menu = QMenu()
        add_action = menu.addAction("New Message")
        del_action = menu.addAction("Delete Selected")
        action = menu.exec_(self.transmit_table.viewport().mapToGlobal(pos))
        if action == add_action:
            dialog = NewMessageDialog()
            if dialog.exec_() == QDialog.Accepted:
                data = dialog.get_data()
                self.add_transmit_row(data)
        elif action == del_action:
            selected = self.transmit_table.currentRow()
            if selected >= 0:
                self.transmit_table.removeRow(selected)

    def add_transmit_row(self, data):
        row = self.transmit_table.rowCount()
        self.transmit_table.insertRow(row)
        enable_box = QCheckBox()
        self.transmit_table.setCellWidget(row, 0, enable_box)
        msg_type = "EXT" if data["extended"] else "STD"
        databytes = " ".join([d if d else "00" for d in data["data"]])
        self.transmit_table.setItem(row, 1, QTableWidgetItem(data["id"] + "h"))
        self.transmit_table.setItem(row, 2, QTableWidgetItem(msg_type))
        self.transmit_table.setItem(row, 3, QTableWidgetItem(str(data["length"])))
        self.transmit_table.setItem(row, 4, QTableWidgetItem(databytes))
        self.transmit_table.setItem(row, 5, QTableWidgetItem(data["cycle"]))
        self.transmit_table.setItem(row, 6, QTableWidgetItem("0"))
        self.transmit_table.setItem(row, 7, QTableWidgetItem(data["comment"]))

    # ----------------------------
    # Connection control
    # ----------------------------
    def toggle_connection(self):
        # Start reader thread (it will attempt to init/reconnect automatically)
        if not self.reader or not self.reader.isRunning():
            self.reader = CANReader(self.pcan, CAN_CHANNEL, CAN_BAUDRATE)
            self.reader.message_received.connect(self.process_message)
            self.reader.status_changed.connect(self.on_hardware_status_changed)
            self.reader.error_occurred.connect(self.on_reader_error)
            self.reader.start()
            self.connect_btn.setText("Disconnect")
            self.status_conn.setText("Connecting...")
            self.status_conn.setStyleSheet("color: orange; font-weight: bold;")
        else:
            # Stop reader and uninitialize
            try:
                self.reader.stop()
                self.reader.wait(2000)
            except Exception:
                pass
            self.reader = None
            try:
                self.pcan.Uninitialize(CAN_CHANNEL)
            except Exception:
                pass
            self.is_connected = False
            self.connect_btn.setText("Connect")
            self.status_conn.setText("Disconnected")
            self.status_conn.setStyleSheet("color: black;")
            self.status_bitrate.setText("Bit rate: ---")
            self.connection_start_time = None

    def on_reader_error(self, msg):
        # show non-fatal errors in status bar
        self.status_bus.setText(f"Reader Error: {msg}")

    def on_hardware_status_changed(self, connected: bool):
        prev = self.is_connected
        self.is_connected = connected

        # Only write events if actual change from previous state
        if connected and not prev:
            self.status_conn.setText("Connected to hardware PCAN-USB")
            self.status_conn.setStyleSheet("color: green; font-weight: bold;")
            self.status_bitrate.setText("Bit rate: 250 kbit/s")
            if self.recording_start_time is None:
                self.connection_start_time = time.time()
            msg = self._format_hw_event_comment("PCAN HARDWARE GOT CONNECTED BACK AT")
            self._log_comment_and_trace(msg)
        elif not connected and prev:
            self.status_conn.setText("Hardware Disconnected")
            self.status_conn.setStyleSheet("color: red; font-weight: bold;")
            self.status_bitrate.setText("Bit rate: ---")
            msg = self._format_hw_event_comment("PCAN HARDWARE GOT DISCONNECTED AT")
            self._log_comment_and_trace(msg)

    def _format_hw_event_comment(self, prefix_text: str) -> str:
        lt = time.localtime()
        millis = int((time.time() % 1) * 1000)
        time_only = time.strftime("%H:%M:%S", lt) + f".{millis}.0"
        comment_line = f"; {prefix_text} {time_only}"
        return comment_line

    def _log_comment_and_trace(self, comment_line: str):
        # write comment into log file (do not close)
        if self.log_handler:
            try:
                self.log_handler.write(comment_line + "\n")
            except Exception:
                self.status_bus.setText("Failed writing log comment")

        # append a visible event row to trace table but via pending queue (smooth)
        lt = time.localtime()
        millis = int((time.time() % 1) * 1000)
        display_time = time.strftime("%H:%M:%S", lt) + f".{millis}"
        row = [display_time, "--", "!", "", comment_line]
        self._pending_trace.append(row)

    def handle_disconnect(self):
        # stop reader and uninitialize
        if self.reader and self.reader.isRunning():
            self.reader.stop()
            self.reader.wait(2000)
            self.reader = None
        try:
            self.pcan.Uninitialize(CAN_CHANNEL)
        except Exception:
            pass
        self.is_connected = False
        self.connect_btn.setText("Connect")
        self.status_conn.setText("Disconnected")
        self.status_conn.setStyleSheet("color: black;")
        self.status_bitrate.setText("Bit rate: ---")
        self.connection_start_time = None

    # ----------------------------
    # Message processing & trace buffering (non-blocking UI)
    # ----------------------------
    def process_message(self, msg, ts_us):
        # Keep the same live-data update logic
        can_id = msg.ID
        length = msg.LEN
        data = ' '.join(f"{b:02X}" for b in msg.DATA[:length])

        # Update live_data and receive table (unchanged)
        if can_id not in self.live_data:
            self.live_data[can_id] = {"count": 1, "last_ts": ts_us, "cycle_time": 0, "data": data}
            row = self.receive_table.rowCount()
            self.receive_table.insertRow(row)
            self.receive_table.setItem(row, 0, QTableWidgetItem(f"{can_id:03X}"))
            self.receive_table.setItem(row, 1, QTableWidgetItem("1"))
            self.receive_table.setItem(row, 2, QTableWidgetItem("0"))
            self.receive_table.setItem(row, 3, QTableWidgetItem(data))
        else:
            old = self.live_data[can_id]
            cycle = (ts_us - old["last_ts"]) / 1000.0
            if cycle < 0:
                cycle = 0
            old["count"] += 1
            old["last_ts"] = ts_us
            old["cycle_time"] = cycle
            old["data"] = data
            # update receive table row
            for row in range(self.receive_table.rowCount()):
                item = self.receive_table.item(row, 0)
                if item and item.text() == f"{can_id:03X}":
                    self.receive_table.setItem(row, 1, QTableWidgetItem(str(old["count"])))
                    self.receive_table.setItem(row, 2, QTableWidgetItem(f"{cycle:.1f}"))
                    self.receive_table.setItem(row, 3, QTableWidgetItem(data))
                    break

        # Trace timestamp selection
        if self.recording_start_time is not None:
            timestamp_s = time.time() - self.recording_start_time
        elif self.connection_start_time is not None:
            timestamp_s = time.time() - self.connection_start_time
        else:
            timestamp_s = ts_us / 1_000_000.0

        display_time = f"{timestamp_s:.4f}"
        trace_row = [display_time, f"{can_id:04X}", "Rx", str(length), data]

        # Enqueue instead of immediate UI insert to keep UI responsive
        self._pending_trace.append(trace_row)

        # Logging: write to TRC immediately (keeps sequence)
        if self.logging and self.log_start_time:
            offset_sec = time.time() - self.log_start_time
            self.message_count += 1
            self.write_trc_entry(self.message_count, offset_sec, msg, tx=False)

    def _flush_pending_trace(self):
        """
        Flushes up to TRACE_ROWS_PER_FLUSH pending rows to the trace_table.
        Ensures trace_table length stays capped to TRACE_ROW_LIMIT.
        """
        rows_this_flush = 0
        while self._pending_trace and rows_this_flush < TRACE_ROWS_PER_FLUSH:
            row = self._pending_trace.popleft()
            # append row to UI
            trace_row_idx = self.trace_table.rowCount()
            self.trace_table.insertRow(trace_row_idx)
            for col, val in enumerate(row):
                self.trace_table.setItem(trace_row_idx, col, QTableWidgetItem(str(val)))
            # append to internal buffer
            self.trace_buffer.append(row)
            # enforce limit: remove oldest rows beyond cap
            while self.trace_table.rowCount() > self.max_trace_messages:
                self.trace_table.removeRow(0)
                if self.trace_buffer:
                    self.trace_buffer.popleft()
            rows_this_flush += 1

        if rows_this_flush:
            # keep view at bottom
            self.trace_table.scrollToBottom()

    # ----------------------------
    # Transmit logic (unchanged behavior, but keep reader from false disconnects)
    # ----------------------------
    def auto_send_messages(self):
        if not self.is_connected:
            return
        now_ms = time.time() * 1000
        if not hasattr(self, "_last_send_times"):
            self._last_send_times = {}
        for row in range(self.transmit_table.rowCount()):
            enable_widget = self.transmit_table.cellWidget(row, 0)
            if not enable_widget or not enable_widget.isChecked():
                continue
            cycle_str = self.transmit_table.item(row, 5).text()
            try:
                cycle = float(cycle_str)
            except Exception:
                cycle = 0
            if cycle <= 0:
                continue
            last_sent = self._last_send_times.get(row, 0)
            if (now_ms - last_sent) >= cycle:
                self._send_can_row(row)
                self._last_send_times[row] = now_ms

    def _send_can_row(self, row):
        try:
            can_id_text = self.transmit_table.item(row, 1).text()
            can_id = int(can_id_text.replace("h", ""), 16)
            data_str = self.transmit_table.item(row, 4).text().strip()
            data_bytes = [int(x, 16) for x in data_str.split() if x]
            length = len(data_bytes)
            msg = TPCANMsg()
            msg.ID = can_id
            msg.LEN = length
            msg.DATA = (c_ubyte * 8)(*data_bytes + [0] * (8 - length))
            msg.MSGTYPE = PCAN_MESSAGE_STANDARD
            result = self.pcan.Write(CAN_CHANNEL, msg)
            if result != PCAN_ERROR_OK:
                self.status_bus.setText(f"Send Error: {result}")
            else:
                count_item = self.transmit_table.item(row, 6)
                if count_item is None:
                    count_item = QTableWidgetItem("0")
                    self.transmit_table.setItem(row, 6, count_item)
                try:
                    count = int(count_item.text())
                except Exception:
                    count = 0
                count += 1
                count_item.setText(str(count))

                # Logging Tx frame
                if self.logging and self.log_start_time:
                    offset_sec = time.time() - self.log_start_time
                    self.message_count += 1
                    self.write_trc_entry(self.message_count, offset_sec, msg, tx=True)

                ts_us = int(time.time() * 1e6)
                data = ' '.join(f"{b:02X}" for b in data_bytes)

                # Add TX to pending trace queue (so UI updates are batched)
                if self.recording_start_time is not None:
                    timestamp_s = time.time() - self.recording_start_time
                elif self.connection_start_time is not None:
                    timestamp_s = time.time() - self.connection_start_time
                else:
                    timestamp_s = ts_us / 1_000_000.0

                display_time = f"{timestamp_s:.4f}"
                trace_row = [display_time, f"{can_id:04X}", "Tx", str(length), data]
                self._pending_trace.append(trace_row)

        except Exception as e:
            self.status_bus.setText(f"Send Exception: {e}")

    # ----------------------------
    # Parse tool wrappers (unchanged)
    # ----------------------------
    def _start_background_task_with_progress(self, target_func):
        progress = QProgressDialog("Parsing file... Please wait.", "Cancel", 0, 0, self)
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Parsing")
        progress.setMinimumDuration(200)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.show()

        worker = WorkerThread(target_func)
        self._worker_thread = worker
        self._progress_dialog = progress

        def on_finished(msg):
            if progress:
                progress.close()
            self._worker_thread = None
            self._progress_dialog = None
            QMessageBox.information(self, "Done", msg if msg else "Conversion completed.")

        def on_error(err):
            if progress:
                progress.close()
            self._worker_thread = None
            self._progress_dialog = None
            QMessageBox.critical(self, "Error", f"Conversion failed: {err}")

        def on_cancel():
            if worker.isRunning():
                QMessageBox.information(self, "Cancel requested", "Cancellation requested. The conversion will stop when possible.")
            progress.setLabelText("Cancellation requested...")

        progress.canceled.connect(on_cancel)
        worker.finished_signal.connect(on_finished)
        worker.error_signal.connect(on_error)
        worker.start()

    def convert_trc_to_csv(self):
        trc_paths, _ = QFileDialog.getOpenFileNames(self, "Select one or more TRC Files", "", "TRC Files (*.trc)")
        if not trc_paths:
            QMessageBox.information(self, "No File Selected", "No TRC file selected. Conversion cancelled.")
            return
        dbc_path, _ = QFileDialog.getOpenFileName(self, "Select DBC File", "", "DBC Files (*.dbc)")
        if not dbc_path:
            QMessageBox.information(self, "No DBC Selected", "No DBC file selected. Conversion cancelled.")
            return
        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No output selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            trc_to_csv(trc_paths, dbc_path, output_path)
            return f"TRC → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    def convert_log_to_csv(self):
        log_path, _ = QFileDialog.getOpenFileName(self, "Select Log File", "", "Log Files (*.log)")
        if not log_path:
            QMessageBox.information(self, "No File Selected", "No LOG file selected. Conversion cancelled.")
            return
        dbc_path, _ = QFileDialog.getOpenFileName(self, "Select DBC File", "", "DBC Files (*.dbc)")
        if not dbc_path:
            QMessageBox.information(self, "No DBC Selected", "No DBC file selected. Conversion cancelled.")
            return
        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No output selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            parse_log_to_compact_csv(log_path, dbc_path, output_path)
            return f"LOG → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    # ----------------------------
    # Logging methods (auto-resume behavior)
    # ----------------------------
    def ask_log_filename(self):
        if not self.is_connected:
            QMessageBox.warning(self, "Not connected", "Please connect to PCAN device before starting logging.")
            return
        filename, _ = QFileDialog.getSaveFileName(self, "Save Log File", "", "TRC Files (*.trc)")
        if filename:
            self.start_logging(filename)

    def start_logging(self, filename):
        try:
            if self.log_handler:
                try:
                    self.log_handler.close()
                except Exception:
                    pass
                self.log_handler = None

            self.log_handler = LogFileHandler(filename)
            self.current_log_filename = filename
            self.log_start_time = time.time()
            self.message_count = 0
            self.header_written = False
            self.write_trc_header()
            self.header_written = True

            # Start recording timestamps at now
            self.recording_start_time = time.time()

            self.logging = True
            self.log_start_btn.setEnabled(False)
            self.log_stop_btn.setEnabled(True)
            self.status_bus.setText(f"Logging Started: {filename}")

            self._blink_state = True
            self._blink_timer.start(500)
        except Exception as e:
            self.status_bus.setText(f"Logging Error: {e}")

    def stop_logging(self):
        self.logging = False
        self._blink_timer.stop()
        self.status_bus.setStyleSheet("color: black;")
        self.status_bus.setText("Logging Stopped")
        self.recording_start_time = None
        if self.log_handler:
            try:
                self.log_handler.close()
            except Exception:
                pass
            self.log_handler = None
        self.current_log_filename = None
        self.log_start_btn.setEnabled(True)
        self.log_stop_btn.setEnabled(False)

    def write_trc_header(self):
        if self.header_written:
            return
        dt_now = time.localtime()
        human_time = time.strftime("%d-%m-%Y %H:%M:%S", dt_now)
        millis = int((time.time() % 1) * 1000)
        epoch_days_fraction = time.time() / 86400
        if self.log_handler:
            self.log_handler.write(
                f";$FILEVERSION=1.1\n"
                f";$STARTTIME={epoch_days_fraction:.10f}\n"
                f";\n"
                f";   Start time: {human_time}.{millis}.0\n"
                f";   Generated by PCAN-View v5.0.1.822\n"
                f";\n"
                f";   Message Number\n"
                f";   |         Time Offset (ms)\n"
                f";   |         |        Type\n"
                f";   |         |        |        ID (hex)\n"
                f";   |         |        |        |     Data Length\n"
                f";   |         |        |        |     |   Data Bytes (hex) ...\n"
                f";   |         |        |        |     |   |\n"
                f";---+--   ----+----  --+--  ----+---  +  -+ -- -- -- -- -- -- --\n"
            )

    def write_trc_entry(self, msg_num, offset_sec, msg, tx=False):
        direction = "Tx" if tx else "Rx"
        data_str = " ".join(f"{b:02X}" for b in msg.DATA[:msg.LEN])
        offset_ms = offset_sec * 1000
        if self.log_handler:
            try:
                self.log_handler.write(
                    f"{msg_num:6}){offset_ms:11.1f}  {direction:<3}        "
                    f"{msg.ID:04X}  {msg.LEN}  {data_str}\n"
                )
            except Exception:
                self.status_bus.setText("Failed writing TRC entry")

    # ----------------------------
    # Blink status text
    # ----------------------------
    def _blink_status_text(self):
        if self.logging:
            if self._blink_state:
                self.status_bus.setStyleSheet("color: red; font-weight: bold;")
            else:
                self.status_bus.setStyleSheet("color: black; font-weight: normal;")
            self._blink_state = not self._blink_state
        else:
            self.status_bus.setStyleSheet("color: black; font-weight: normal;")
            self._blink_timer.stop()

    # ----------------------------
    # Small helper to switch to Trace tab (was missing and caused AttributeError)
    # ----------------------------
    def switch_to_trace_tab(self):
        self.tabs.setCurrentWidget(self.trace_tab)


if __name__ == "__main__":
    LOCAL_VERSION = "1.0.0"
    app = QApplication(sys.argv)
    updater.check_for_update(LOCAL_VERSION, app)
    window = PCANViewClone()
    window.show()
    sys.exit(app.exec())
