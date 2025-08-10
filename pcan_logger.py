import sys
import time
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
from PCANBasic import *
import updater  # Import updater module

CAN_CHANNEL = PCAN_USBBUS1
CAN_BAUDRATE = PCAN_BAUD_250K

# ----------------------------
# Worker Thread for Receiving CAN Messages
# ----------------------------
class CANReader(QThread):
    message_received = Signal(object, object)
    disconnected = Signal()

    def __init__(self, pcan, channel):
        super().__init__()
        self.pcan = pcan
        self.channel = channel
        self.running = True

    def run(self):
        while self.running:
            sts = self.pcan.GetStatus(self.channel)
            if sts != PCAN_ERROR_OK:
                self.disconnected.emit()
                break
            result, msg, timestamp = self.pcan.Read(self.channel)
            if result == PCAN_ERROR_OK:
                ts_us = timestamp.micros + timestamp.millis * 1000
                self.message_received.emit(msg, ts_us)
            else:
                time.sleep(0.005)

# ----------------------------
# Generic Worker Thread to run long blocking parse_tool functions
# ----------------------------
class WorkerThread(QThread):
    finished_signal = Signal(str)   # message to show on completion
    error_signal = Signal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            if isinstance(result, str) and result:
                msg = result
            else:
                msg = "Conversion completed."
            self.finished_signal.emit(msg)
        except Exception as e:
            self.error_signal.emit(str(e))

# ----------------------------
# Popup dialog for New Transmit Message
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
# Main Window
# ----------------------------
class PCANViewClone(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PCAN-View (Logger & DebugTools)")
        self.resize(1300, 700)

        # Initialize PCANBasic
        self.pcan = PCANBasic()
        self.worker = None
        self.is_connected = False
        self.live_data = {}
        self.log_file = None
        self.log_start_time = None
        self.message_count = 0
        self.logging = False

        # --- Toolbar ---
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

        # --- Tabs (like PCAN-View) ---
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # --- Tab 1: Receive / Transmit ---
        self.recv_tx_tab = QWidget()
        self.setup_recv_tx_tab()
        self.tabs.addTab(self.recv_tx_tab, "Receive / Transmit")

        # --- Tab 2: Trace ---
        self.trace_tab = QWidget()
        self.setup_trace_tab()
        self.tabs.addTab(self.trace_tab, "Trace")

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        self.status_conn = QLabel("Disconnected")
        self.status_bitrate = QLabel("Bit rate: ---")
        self.status_bus = QLabel("Status: ---")
        self.status_bar.addWidget(self.status_conn)
        self.status_bar.addWidget(self.status_bitrate)
        self.status_bar.addWidget(self.status_bus)
        self.setStatusBar(self.status_bar)

        # Add Parse File toolbutton on the right side of the tab bar
        self.parse_toolbutton = QToolButton()
        self.parse_toolbutton.setText("Parse File")
        self.parse_toolbutton.setStyleSheet(
            "QToolButton { background-color: green; color: white; font-weight: bold; padding: 6px; }"
            "QToolButton:pressed { background-color: darkgreen; }"
            "QToolButton:hover { background-color: green; }"
        )
        self.parse_menu = QMenu(self.parse_toolbutton)

        # Create QAction widgets with fixed background colors (no hover effect)
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

        self.parse_menu.addAction(create_colored_action("TRC → CSV", "#dc0d33"))  # firebrick red
        self.parse_menu.addAction(create_colored_action("LOG → CSV", "#09ad3d"))  # green

        self.parse_toolbutton.setMenu(self.parse_menu)
        self.parse_toolbutton.setPopupMode(QToolButton.InstantPopup)

        # Place parse button on the tab bar (right corner)
        self.tabs.setCornerWidget(self.parse_toolbutton, Qt.TopRightCorner)

        # Styling
        self.setStyleSheet("""
            QMainWindow { background-color: #f0f0f0; }
            QTableWidget { background: white; alternate-background-color: #e6f2ff; gridline-color: #c0c0c0; }
            QHeaderView::section { background-color: #0078D7; color: white; padding: 4px; }
        """)

        # Auto Send Timer
        self.auto_send_timer = QTimer()
        self.auto_send_timer.timeout.connect(self.auto_send_messages)
        self.auto_send_timer.start(100)

        # Trace buffer
        self.trace_buffer = []
        self.max_trace_messages = 2000

        # Keep a reference to worker & progress dialog so they don't get garbage-collected
        self._worker_thread = None
        self._progress_dialog = None

    # Helper for Parse menu actions
    def _parse_menu_action_triggered(self, text):
        if text == "TRC → CSV":
            self.convert_trc_to_csv()
        elif text == "LOG → CSV":
            self.convert_log_to_csv()

    # ----------------------------
    # UI Setup
    # ----------------------------
    def setup_recv_tx_tab(self):
        layout = QVBoxLayout()
        splitter = QSplitter(Qt.Vertical)

        # --- Receive Section ---
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

        # --- Transmit Section ---
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
        splitter.setSizes([350, 350])  # Default sizes
        layout.addWidget(splitter)
        self.recv_tx_tab.setLayout(layout)

    def setup_trace_tab(self):
        layout = QVBoxLayout()
        self.trace_table = QTableWidget()
        self.trace_table.setColumnCount(5)
        self.trace_table.setHorizontalHeaderLabels(["Time (ms)", "CAN ID", "Rx/Tx", "Length", "Data"])
        self.trace_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.trace_table.setAlternatingRowColors(True)
        layout.addWidget(self.trace_table)
        self.trace_tab.setLayout(layout)

    def switch_to_trace_tab(self):
        self.tabs.setCurrentWidget(self.trace_tab)

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
        # Fix: Ensure Count cell exists as QTableWidgetItem with initial "0"
        self.transmit_table.setItem(row, 6, QTableWidgetItem("0"))
        self.transmit_table.setItem(row, 7, QTableWidgetItem(data["comment"]))

    # ----------------------------
    # Connection and CAN message processing
    # ----------------------------
    def toggle_connection(self):
        if not self.is_connected:
            result = self.pcan.Initialize(CAN_CHANNEL, CAN_BAUDRATE)
            if result == PCAN_ERROR_OK:
                self.worker = CANReader(self.pcan, CAN_CHANNEL)
                self.worker.message_received.connect(self.process_message)
                self.worker.disconnected.connect(self.handle_disconnect)
                self.worker.start()
                self.is_connected = True
                self.connect_btn.setText("Disconnect")
                self.status_conn.setText("Connected to hardware PCAN-USB")
                self.status_conn.setStyleSheet("color: green; font-weight: bold;")
                self.status_bitrate.setText("Bit rate: 250 kbit/s")
            else:
                self.status_conn.setText("No device or busy")
                self.status_conn.setStyleSheet("color: red;")
        else:
            self.handle_disconnect()

    def handle_disconnect(self):
        if self.worker:
            self.worker.running = False
            self.worker.wait()
        self.pcan.Uninitialize(CAN_CHANNEL)
        self.is_connected = False
        self.connect_btn.setText("Connect")
        self.status_conn.setText("Disconnected")
        self.status_conn.setStyleSheet("color: red;")
        self.status_bus.setText("Status: ---")

    def process_message(self, msg, ts_us):
        can_id = f"{msg.ID:04X}"
        data = ' '.join(f"{b:02X}" for b in msg.DATA[:msg.LEN])
        # --- Update Rx table ---
        if can_id in self.live_data:
            row, count, last_ts = self.live_data[can_id]
            cycle_time = (ts_us - last_ts) / 1000.0
            count += 1
            self.receive_table.setItem(row, 1, QTableWidgetItem(str(count)))
            self.receive_table.setItem(row, 2, QTableWidgetItem(f"{cycle_time:.2f}"))
            self.receive_table.setItem(row, 3, QTableWidgetItem(data))
            self.live_data[can_id] = (row, count, ts_us)
        else:
            row = self.receive_table.rowCount()
            self.receive_table.insertRow(row)
            self.receive_table.setItem(row, 0, QTableWidgetItem(can_id))
            self.receive_table.setItem(row, 1, QTableWidgetItem("1"))
            self.receive_table.setItem(row, 2, QTableWidgetItem("0.00"))
            self.receive_table.setItem(row, 3, QTableWidgetItem(data))
            self.live_data[can_id] = (row, 1, ts_us)

        # --- Trace logging buffer ---
        self.add_trace_entry(ts_us, can_id, "Rx", msg.LEN, data)
        if self.logging:
            self.message_count += 1
            offset_sec = time.time() - self.log_start_time
            self.write_trc_entry(self.message_count, offset_sec, msg, tx=False)

    def auto_send_messages(self):
        if not self.is_connected:
            return
        current_time = int(time.time() * 1000)
        for row in range(self.transmit_table.rowCount()):
            chk = self.transmit_table.cellWidget(row, 0)
            if chk and chk.isChecked():
                try:
                    cycle_time = int(self.transmit_table.item(row, 5).text())
                except Exception:
                    cycle_time = 100
                # Instead of current_time % cycle_time < 100, use elapsed time logic for better accuracy
                last_sent_key = f"last_sent_{row}"
                last_sent = getattr(self, last_sent_key, 0)
                if current_time - last_sent >= cycle_time:
                    self._send_can_row(row)
                    setattr(self, last_sent_key, current_time)

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
                # Update Count cell on transmit table
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

                # Log transmitted message if logging active
                if self.logging:
                    self.message_count += 1
                    offset_sec = time.time() - self.log_start_time
                    self.write_trc_entry(self.message_count, offset_sec, msg, tx=True)

                # Add to trace buffer
                ts_us = int(time.time() * 1e6)
                data = ' '.join(f"{b:02X}" for b in data_bytes)
                self.add_trace_entry(ts_us, f"{can_id:04X}", "Tx", length, data)
        except Exception as e:
            self.status_bus.setText(f"Send Exception: {e}")

    # ----------------------------
    # Trace buffer & table
    # ----------------------------
    def add_trace_entry(self, ts_us, can_id, direction, length, data):
        timestamp_ms = ts_us / 1000.0
        self.trace_buffer.append([f"{timestamp_ms:.3f}", can_id, direction, str(length), data])
        if len(self.trace_buffer) > self.max_trace_messages:
            self.trace_buffer.pop(0)
        if self.tabs.currentWidget() == self.trace_tab:
            self.refresh_trace_table()

    def refresh_trace_table(self):
        self.trace_table.setRowCount(len(self.trace_buffer))
        for i, entry in enumerate(self.trace_buffer):
            for j, val in enumerate(entry):
                self.trace_table.setItem(i, j, QTableWidgetItem(val))

    # ----------------------------
    # Parse tool functions with fixed dialogs and progress
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
        trc_path, _ = QFileDialog.getOpenFileName(self, "Select TRC File", "", "TRC Files (*.trc)")
        if not trc_path:
            QMessageBox.information(self, "No File Selected", "No TRC file selected. Conversion cancelled.")
            return

        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No File Selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            trc_to_csv(trc_path, output_path)
            return f"TRC → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    def convert_log_to_csv(self):
        log_path, _ = QFileDialog.getOpenFileName(self, "Select Log File", "", "Log Files (*.log)")
        if not log_path:
            QMessageBox.information(self, "No File Selected", "No LOG file selected. Conversion cancelled.")
            return

        dbc_path, _ = QFileDialog.getOpenFileName(self, "Select DBC File", "", "DBC Files (*.dbc)")
        if not dbc_path:
            QMessageBox.information(self, "No File Selected", "No DBC file selected. Conversion cancelled.")
            return

        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No File Selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            parse_log_to_compact_csv(log_path, dbc_path, output_path)
            return f"LOG → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    # ----------------------------
    # Logging methods
    # ----------------------------
    def ask_log_filename(self):
        filename, _ = QFileDialog.getSaveFileName(self, "Save Log File", "", "TRC Files (*.trc)")
        if filename:
            self.start_logging(filename)

    def start_logging(self, filename):
        try:
            self.log_file = open(filename, "w")
            self.log_start_time = time.time()
            self.message_count = 0
            self.write_trc_header()
            self.logging = True
            self.log_start_btn.setEnabled(False)
            self.log_stop_btn.setEnabled(True)
            self.status_bus.setText("Logging Started")
        except Exception as e:
            self.status_bus.setText(f"Logging Error: {e}")

    def stop_logging(self):
        self.logging = False
        if self.log_file:
            self.log_file.close()
            self.log_file = None
        self.log_start_btn.setEnabled(True)
        self.log_stop_btn.setEnabled(False)
        self.status_bus.setText("Logging Stopped")

    def write_trc_header(self):
        dt_now = time.localtime()
        timestamp_str = time.strftime("%d-%m-%Y %H:%M:%S", dt_now)
        epoch_days = int(time.time() // 86400)
        if self.log_file:
            self.log_file.write(
                ";$FILEVERSION=2.0\n"
                f";$STARTTIME={timestamp_str},{epoch_days}\n"
                ";$COLUMNS=N,O,T,I,d,L,D\n"
            )

    def write_trc_entry(self, msg_num, offset_sec, msg, tx=False):
        direction = "Tx" if tx else "Rx"
        data_str = ' '.join(f"{b:02X}" for b in msg.DATA[:msg.LEN])
        if self.log_file:
            self.log_file.write(
                f"{msg_num} {offset_sec:.6f} {direction} {msg.ID:03X} "
                f"v {msg.LEN} {data_str}\n"
            )


if __name__ == "__main__":
    LOCAL_VERSION = "1.0.0"  # keep in sync with your app version
    app = QApplication(sys.argv)  # Create QApplication first
    updater.check_for_update(LOCAL_VERSION, app)  # Pass app instance here
    window = PCANViewClone()
    window.show()
    sys.exit(app.exec())

