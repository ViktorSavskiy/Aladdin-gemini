"""
ШАБЛОН КОНФИГУРАЦИИ ДОСТУПОВ

Инструкция:
1. Скопируйте этот файл и назовите копию 'credentials.py'.
2. Заполните реальные данные в 'credentials.py'.
3. УБЕДИТЕСЬ, что 'credentials.py' добавлен в .gitignore!
"""

# --- Источники данных ---

# CoinMarketCap API (https://pro.coinmarketcap.com/account)
# Оставьте None, если используете только CoinGecko или Binance
COINMARKETCAP_API_KEY = None  # Пример: "b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c"

# CoinGecko API (опционально, для Pro аккаунтов)
# Для бесплатного использования оставьте None
COINGECKO_API_KEY = None 

# --- Торговля и Биржи ---

# Binance API (https://www.binance.com/en/my/settings/api-management)
# Внимание: Включайте только права на чтение (Read Only), если не планируете авто-торговлю
BINANCE_API_KEY = None
BINANCE_API_SECRET = None

# --- Уведомления ---

# Telegram Bot (через @BotFather)
TELEGRAM_BOT_TOKEN = None  # Пример: "123456789:ABCdefGHIjklMNOpqrstUVwxyz"
TELEGRAM_CHAT_ID = None    # Ваш ID (можно узнать через ботов, например @userinfobot)

# --- Сеть ---

# Прокси (если вы в РФ или работаете с серверами, блокирующими ваш IP)
# Формат: "http://user:password@ip:port"
# Если прокси не нужен, оставьте словарь пустым или None
PROXY = None
# Пример заполнения:
# PROXY = {
#     "http": "http://user:pass@1.2.3.4:8080",
#     "https": "http://user:pass@1.2.3.4:8080",
# }

"""
ШАБЛОН КОНФИГУРАЦИИ ДОСТУПОВ
Скопируйте в credentials.py
"""

# --- On-Chain Источники (НОВОЕ) ---

# Glassnode (https://glassnode.com) - Лучший, но дорогой
GLASSNODE_API_KEY = None

# Bitquery (https://bitquery.io) - Хорош для DEX
BITQUERY_API_KEY = None

# CryptoQuant (https://cryptoquant.com) - Потоки бирж
CRYPTOQUANT_API_KEY = None

# Messari (https://messari.io) - Фундаментальные метрики (есть Free tier)
MESSARI_API_KEY = None 

# --- Уведомления ---
TELEGRAM_BOT_TOKEN = None
TELEGRAM_CHAT_ID = None