import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env (если есть)
load_dotenv()

class Config:
    # --- Пути ---
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    DB_DIR = DATA_DIR / "database"
    LOG_DIR = BASE_DIR / "logs"  # Отдельная переменная для папки логов
    
    DB_PATH = DB_DIR / "crypto_aladdin.db"
    LOG_FILE = LOG_DIR / "crypto_aladdin.log"

    # --- Фильтры ---
    MIN_MARKET_CAP = 1_000_000_000  # $1B
    MIN_VOLUME_24H = 10_000_000     # $10M

    # --- Сбор данных ---
    HISTORICAL_DAYS = 90
    UPDATE_INTERVAL_HOURS = 24
    
    # --- API Keys (Берем из переменных окружения для безопасности) ---
    CMC_API_KEY = os.getenv("CMC_API_KEY")
    COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

    # --- Источники данных ---
    # Можно реализовать логику: если есть ключ, то True, иначе False
    DATA_SOURCES = {
        "coinmarketcap": bool(CMC_API_KEY), 
        "coingecko": True,   # Обычно есть бесплатный публичный API без ключа
        "binance": False,
    }

    # --- Лимиты API ---
    # CoinGecko Public API: ~10-30 запросов/мин (зависит от IP)
    # CoinMarketCap Free: 333 запросов/день
    API_RATE_LIMITS = {
        "coingecko": 5,      # Понизил для безопасности (Public API строже)
        "coinmarketcap": 333,
    }

    # --- Логирование ---
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # --- Метрики ---
    METRIC_WINDOWS = {
        "volatility": 30,
        "returns": [7, 30],
        "correlation": 30,
    }

    @classmethod
    def setup_directories(cls):
        """Создает необходимую структуру директорий"""
        directories = [
            cls.RAW_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.DB_DIR,
            cls.LOG_DIR  # Добавили папку логов
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"Directory verified: {directory}")

# Инициализация при импорте модуля
Config.setup_directories()

# Пример доступа:
# print(Config.DB_PATH)