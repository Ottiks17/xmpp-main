"""
XMPP Messaging Client
Main entry point
"""

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def run():
    from gui_app import main

    main()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        try:
            from PyQt5.QtWidgets import QApplication, QMessageBox

            app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(
                None,
                "Ошибка запуска",
                f"Не удалось запустить приложение:\n\n{exc}",
            )
        except Exception:
            print(f"Ошибка запуска: {exc}", file=sys.stderr)
        sys.exit(1)

