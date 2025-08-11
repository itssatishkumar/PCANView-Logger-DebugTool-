import os
import sys
import requests
from PySide6.QtWidgets import QApplication, QMessageBox, QProgressDialog
from PySide6.QtCore import Qt

def get_online_version(version_url):
    try:
        r = requests.get(version_url, timeout=10)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        print("Failed to fetch version info:", e)
        return None

def download_file(url, target_path, parent=None):
    try:
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))

        progress = QProgressDialog(f"Downloading {os.path.basename(target_path)}...", "Cancel", 0, total, parent)
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Updater")
        progress.show()

        downloaded = 0
        chunk_size = 8192
        with open(target_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    progress.setValue(downloaded)
                    QApplication.processEvents()
                    if progress.wasCanceled():
                        progress.close()
                        return False
        progress.close()
        return True
    except Exception as e:
        print(f"Download failed for {url}: {e}")
        return False

def check_for_update(local_version,
                     app,
                     version_url="https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/version.txt"):

    online_version = get_online_version(version_url)
    if online_version is None:
        return  # Cannot check updates, continue normal startup

    if online_version == local_version:
        return  # Up to date

    reply = QMessageBox.question(None, "Update available",
                                 f"A new version {online_version} is available.\n"
                                 f"Do you want to download and install the update?",
                                 QMessageBox.Yes | QMessageBox.No)
    if reply != QMessageBox.Yes:
        return  # User declined update

    # List of files to update (raw URL, local filename)
    files_to_update = [
        ("https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/pcan_logger.py", "pcan_logger.py"),
        ("https://raw.githubusercontent.com/itssatishkumar/PCANView-Logger-DebugTool-/main/parse_tool.py", "parse_tool.py"),
        # Add more files here as needed
    ]

    target_folder = os.path.dirname(os.path.abspath(sys.argv[0]))

    for file_url, local_name in files_to_update:
        local_path = os.path.join(target_folder, local_name)
        success = download_file(file_url, local_path, parent=None)
        if not success:
            QMessageBox.warning(None, "Update failed", f"Failed to download {local_name}")
            return

    # Update local version file
    version_file_path = os.path.join(target_folder, "version.txt")
    try:
        with open(version_file_path, "w") as f:
            f.write(online_version)
    except Exception as e:
        print("Failed to update local version file:", e)

    QMessageBox.information(None, "Update complete", "Update installed successfully.\nPlease restart the application.")
    sys.exit(0)
