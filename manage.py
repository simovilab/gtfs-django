#!/usr/bin/env python
import os
import sys
from pathlib import Path

def main():
    """Django's command-line utility for administrative tasks."""
    # Root directory (project base)
    BASE_DIR = Path(__file__).resolve().parent

    # Default settings — usamos tests/settings.py como configuración local
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tests.settings')

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Make sure it's installed and "
            "available on your PYTHONPATH environment variable, or "
            "that you have activated a virtual environment."
        ) from exc

    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
