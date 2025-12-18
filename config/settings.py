import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Попытка импорта локальных настроек (если есть credentials.py)
try:
    import credentials
except ImportError:
    credentials = None

class Config:
    # --- Пути (Без изменений) ---
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    DB_DIR = DATA_DIR / "database"
    LOG_DIR = BASE_DIR / "logs"
    
    DB_PATH = DB_DIR / "crypto_aladdin.db"
    LOG_FILE = LOG_DIR / "crypto_aladdin.log"

    # --- Фильтры (Без изменений) ---
    MIN_MARKET_CAP = 1_000_000_000
    MIN_VOLUME_24H = 10_000_000

    # --- Основной сбор данных ---
    HISTORICAL_DAYS = 365 * 2
    UPDATE_INTERVAL_HOURS = 24
    
    # --- API Keys (Базовые) ---
    CMC_API_KEY = os.getenv("CMC_API_KEY") or getattr(credentials, 'COINMARKETCAP_API_KEY', None)
    COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY") or getattr(credentials, 'COINGECKO_API_KEY', None)

    # --- API Keys (On-Chain) - НОВОЕ ---
    GLASSNODE_API_KEY = os.getenv("GLASSNODE_API_KEY") or getattr(credentials, 'GLASSNODE_API_KEY', None)
    BITQUERY_API_KEY = os.getenv("BITQUERY_API_KEY") or getattr(credentials, 'BITQUERY_API_KEY', None)
    CRYPTOQUANT_API_KEY = os.getenv("CRYPTOQUANT_API_KEY") or getattr(credentials, 'CRYPTOQUANT_API_KEY', None)
    MESSARI_API_KEY = os.getenv("MESSARI_API_KEY") or getattr(credentials, 'MESSARI_API_KEY', None)

    # --- Источники данных (Обновлено) ---
    DATA_SOURCES = {
        "coinmarketcap": bool(CMC_API_KEY), 
        "coingecko": True,
        "binance": False,
    }

    # ==================== ON-CHAIN НАСТРОЙКИ (НОВОЕ) ====================
    
    # Определяем доступность источников на основе наличия ключей
    ONCHAIN_SOURCES = {
        "glassnode": bool(GLASSNODE_API_KEY),
        "bitquery": bool(BITQUERY_API_KEY),
        "cryptoquant": bool(CRYPTOQUANT_API_KEY),
        "messari": True,  # У Messari есть бесплатные публичные эндпоинты
        "coingecko_onchain": True  # Dev activity, Community stats
    }

    # Какие метрики пытаемся собрать
    ONCHAIN_METRICS = [
        "active_addresses",      # Активность сети
        "transactions_count",    # Нагрузка
        "transaction_volume",    # Объем переводов ($)
        "fees_volume",           # Комиссии (популярность ETH/BTC)
        "hash_rate",            # Безопасность (PoW)
        "tvl",                  # Total Value Locked (для DeFi)
        "developer_activity"    # Коммиты на GitHub (через CoinGecko)
    ]

    # Глубина истории для on-chain (обычно она весит меньше, но запрашивается дольше)
    ONCHAIN_HISTORY_DAYS = 30

    # --- Лимиты API ---
    API_RATE_LIMITS = {
        "coingecko": 5,  # Консервативный лимит
        "messari": 5,    # Примерный лимит для Messari Free
    }

    # --- Логирование ---
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # --- Метрики анализа ---
    METRIC_WINDOWS = {
        "volatility": 30,
        "returns": [7, 30],
        "correlation": 30,
    }

    @classmethod
    def setup_directories(cls):
        """Создает необходимую структуру директорий"""
        directories = [cls.RAW_DATA_DIR, cls.PROCESSED_DATA_DIR, cls.DB_DIR, cls.LOG_DIR]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
            
# config/settings.py

# ... (существующий код)

# ==================== КЛАССИФИКАЦИЯ АКТИВОВ ====================
# Расширяем списки популярных монет для тестов
    BLOCKCHAIN_CATEGORIES = {
    'L1': ['bitcoin', 'ethereum', 'solana', 'avalanche-2', 'cardano', 'polkadot', 'binancecoin', 'tron'],
    'L2': ['arbitrum', 'optimism', 'polygon-pos', 'base', 'mantle', 'starknet'],
    'DeFi': ['uniswap', 'aave', 'maker', 'lido-dao', 'curve-dao-token', 'pendle', 'gmx'],
    'NFT_Gaming': ['apecoin', 'axie-infinity', 'the-sandbox', 'decentraland', 'gala', 'render-token'],
    'Meme': ['dogecoin', 'shiba-inu', 'pepe', 'bonk', 'floki', 'wif']
}

# ==================== СПЕЦИФИЧНЫЕ ИСТОЧНИКИ ====================
    SPECIFIC_DATA_SOURCES = {
    'defillama': True,       # TVL, Fees, Revenue (Бесплатно!)
    'tokenterminal': False,  # Нужен ключ (дорого)
    'dappradar': False,      # Нужен ключ
    'lunarcrush': False      # Нужен ключ
}

# Метрики, которые мы будем искать в DefiLlama
    DEFILLAMA_METRICS = [
    'tvl',           # Total Value Locked
    'mcap',          # Market Cap (для сверки)
    'fees',          # Fees (24h)
    'revenue',       # Revenue (24h)
    'volume'         # DEX Volume (24h)
]


# Настройки Бэктеста
    BACKTEST_CONFIG = {
    'initial_capital': 10000,  # $10,000 старт
    'fee_rate': 0.001,         # 0.1% комиссия биржи
    'rebalance_period': 7,     # Ребалансировка раз в 7 дней
    'top_n': 10                # Сколько монет держим в портфеле
}
# Инициализация
Config.setup_directories()