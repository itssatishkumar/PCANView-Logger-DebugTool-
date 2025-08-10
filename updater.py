import os
import sys
import requests
import zipfile
import io
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

def download_update(update_url, parent=None):
    try:
        r = requests.get(update_url, stream=True, timeout=30)
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        data = b""

        progress = QProgressDialog("Downloading update...", "Cancel", 0, total, parent)
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Updater")
        progress.show()

        downloaded = 0
        chunk_size = 8192
        for chunk in r.iter_content(chunk_size):
            if chunk:
                data += chunk
                downloaded += len(chunk)
                progress.setValue(downloaded)
                QApplication.processEvents()
                if progress.wasCanceled():
                    return None
        progress.close()
        return data
    except Exception as e:
        print("Download failed:", e)
        return None

def install_update(zip_data, target_folder):
    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
            # Extract all with overwrite
            for member in z.infolist():
                # Compose full path
                extracted_path = os.path.join(target_folder, member.filename)
                if member.is_dir():
                    os.makedirs(extracted_path, exist_ok=True)
                else:
                    # Make sure folder exists
                    os.makedirs(os.path.dirname(extracted_path), exist_ok=True)
                    with open(extracted_path, "wb") as f:
                        f.write(z.read(member.filename))
        return True
    except Exception as e:
        print("Installation failed:", e)
        return False

def check_for_update(local_version,
                     version_url="https://raw.githubusercontent.com/yourusername/yourrepo/main/version.txt",
                     update_url="https://github.com/yourusername/yourrepo/releases/download/v1.0.1/update.zip"):
    app = QApplication.instance()
    if app is None:
        app = QApplication([])

    online_version = get_online_version(version_url)
    if online_version is None:
        return  # Cannot check updates, continue normal startup

    if online_version == local_version:
        return  # Up to date

    # Ask user to update
    reply = QMessageBox.question(None, "Update available",
                                 f"A new version {online_version} is available.\n"
                                 f"Do you want to download and install the update?",
                                 QMessageBox.Yes | QMessageBox.No)
    if reply != QMessageBox.Yes:
        return  # User declined update

    # Download update
    zip_data = download_update(update_url, parent=None)
    if zip_data is None:
        QMessageBox.warning(None, "Update failed", "Download was cancelled or failed.")
        return

    # Install update
    target_folder = os.path.dirname(os.path.abspath(sys.argv[0]))
    success = install_update(zip_data, target_folder)
    if success:
        QMessageBox.information(None, "Update complete", "Update installed successfully.\nPlease restart the application.")
        sys.exit(0)
    else:
        QMessageBox.critical(None, "Update failed", "Failed to install update.")

