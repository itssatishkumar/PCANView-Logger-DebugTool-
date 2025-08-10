import sys
import time
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, 
    QVBoxLayout, QWidget, QSplitter, QMenuBar, QStatusBar, QLabel,
    QToolBar, QAction, QPushButton, QHBoxLayout, QLineEdit, QHeaderView
)
from PySide6.QtCore import Qt, QThread, Signal
from PCANBasic import *

# ----------------------------
# Worker Thread for Receiving CAN Messages
# ----------------------------
class CANReader(QThread):
    message_received = Signal(str, str, str, str)

    def __init__(self, pcan, channel):
        super().__init__()
        self.pcan = pcan
        self.channel = channel
        self.running = True

    def run(self):
        while self.running:
            result, msg, timestamp = self.pcan.Read(self.channel)
            if result == PCAN_ERROR_OK:
                ts = f"{timestamp.micros}"
                cid = hex(msg.ID)
                dlc = str(msg.LEN)
                data = " ".join([hex(b) for b in msg.DATA[:msg.LEN]])
                self.message_received.emit(ts, cid, dlc, data)
            time.sleep(0.005)

# ----------------------------
# Main Window
# ----------------------------
class PCANViewClone(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PCAN-View Clone")
        self.resize(1100, 600)

        # Initialize PCANBasic
        self.pcan = PCANBasic()
        self.channel = PCAN_USBBUS1
        self.worker = None
        self.is_connected = False

        # --- Menu Bar ---
        menubar = self.menuBar()
        menubar.addMenu("File")
        menubar.addMenu("CAN")
        menubar.addMenu("Edit")
        menubar.addMenu("Transmit")
        menubar.addMenu("View")
        menubar.addMenu("Trace")
        menubar.addMenu("Window")
        menubar.addMenu("Help")

        # --- Toolbar ---
        toolbar = QToolBar("Main Toolbar")
        self.addToolBar(toolbar)
        connect_action = QAction("Connect", self)
        connect_action.triggered.connect(self.toggle_connection)
        toolbar.addAction(connect_action)

        # --- Receive Table ---
        self.receive_table = QTableWidget()
        self.receive_table.setColumnCount(4)
        self.receive_table.setHorizontalHeaderLabels(["Timestamp", "CAN ID", "DLC", "Data"])
        self.receive_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.receive_table.setAlternatingRowColors(True)

        # --- Transmit Table ---
        self.transmit_table = QTableWidget()
        self.transmit_table.setColumnCount(7)
        self.transmit_table.setHorizontalHeaderLabels(["CAN-ID", "Type", "Length", "Data", "Cycle Time", "Count", "Comment"])
        self.transmit_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.transmit_table.setAlternatingRowColors(True)

        # Add sample row for transmit
        self.transmit_table.insertRow(0)
        self.transmit_table.setItem(0, 0, QTableWidgetItem("100h"))
        self.transmit_table.setItem(0, 1, QTableWidgetItem("STD"))
        self.transmit_table.setItem(0, 2, QTableWidgetItem("8"))
        self.transmit_table.setItem(0, 3, QTableWidgetItem("00 00 00 00 00 00 00 00"))
        self.transmit_table.setItem(0, 4, QTableWidgetItem("100"))
        self.transmit_table.setItem(0, 5, QTableWidgetItem("0"))
        self.transmit_table.setItem(0, 6, QTableWidgetItem("Manual Send"))

        # Send button
        send_btn = QPushButton("Send Selected")
        send_btn.clicked.connect(self.send_message)
        transmit_layout = QVBoxLayout()
        transmit_layout.addWidget(self.transmit_table)
        transmit_layout.addWidget(send_btn)
        transmit_widget = QWidget()
        transmit_widget.setLayout(transmit_layout)

        # Splitter for layout
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self.receive_table)
        splitter.addWidget(transmit_widget)

        layout = QVBoxLayout()
        layout.addWidget(splitter)
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        self.status_device = QLabel("Disconnected")
        self.status_bitrate = QLabel("Bitrate: 500 kbit/s")
        self.status_bus = QLabel("Status: ---")
        self.status_errors = QLabel("Errors: 0")
        self.status_bar.addWidget(self.status_device)
        self.status_bar.addWidget(self.status_bitrate)
        self.status_bar.addWidget(self.status_bus)
        self.status_bar.addWidget(self.status_errors)
        self.setStatusBar(self.status_bar)

        # Apply simple styling
        self.setStyleSheet("""
            QMainWindow { background-color: #f0f0f0; }
            QTableWidget { background: white; alternate-background-color: #e6f2ff; }
            QStatusBar QLabel { margin-left: 15px; }
        """)

    def toggle_connection(self):
        if not self.is_connected:
            result = self.pcan.Initialize(self.channel, PCAN_BAUD_500K)
            if result == PCAN_ERROR_OK:
                self.worker = CANReader(self.pcan, self.channel)
                self.worker.message_received.connect(self.add_message)
                self.worker.start()
                self.is_connected = True
                self.status_device.setText("Connected: PCAN-USB")
                self.status_bus.setText("Status: OK")
            else:
                self.status_device.setText("No device or busy")
        else:
            if self.worker:
                self.worker.running = False
                self.worker.wait()
            self.pcan.Uninitialize(self.channel)
            self.is_connected = False
            self.status_device.setText("Disconnected")
            self.status_bus.setText("Status: ---")

    def add_message(self, ts, cid, dlc, data):
        row = self.receive_table.rowCount()
        self.receive_table.insertRow(row)
        self.receive_table.setItem(row, 0, QTableWidgetItem(ts))
        self.receive_table.setItem(row, 1, QTableWidgetItem(cid))
        self.receive_table.setItem(row, 2, QTableWidgetItem(dlc))
        self.receive_table.setItem(row, 3, QTableWidgetItem(data))
        self.receive_table.scrollToBottom()

    def send_message(self):
        if not self.is_connected:
            self.status_bus.setText("Status: Connect first")
            return
        selected = self.transmit_table.currentRow()
        if selected < 0:
            return
        try:
            can_id = int(self.transmit_table.item(selected, 0).text().replace("h",""), 16)
            data_str = self.transmit_table.item(selected, 3).text().split()
            data_bytes = bytes(int(b, 16) for b in data_str)
            msg = TPCANMsg()
            msg.ID = can_id
            msg.LEN = len(data_bytes)
            msg.MSGTYPE = PCAN_MESSAGE_STANDARD
            msg.DATA = data_bytes + bytes(8 - len(data_bytes))
            self.pcan.Write(self.channel, msg)
            self.status_bus.setText("Status: Message Sent")
        except Exception as e:
            self.status_bus.setText(f"Error: {e}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = PCANViewClone()
    win.show()
    sys.exit(app.exec())
