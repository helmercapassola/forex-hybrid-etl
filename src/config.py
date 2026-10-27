import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)

TIINGO_API_TOKEN = os.getenv("TIINGO_API_TOKEN")
FOREX_SYMBOL = os.getenv("FOREX_SYMBOL", "EURUSD")

if not TIINGO_API_TOKEN:
    raise ValueError(
        "TIINGO_API_TOKEN não está definido no ficheiro .env"
    )
