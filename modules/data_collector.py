import ccxt
import pandas as pd
import logging

logger = logging.getLogger("Alladin")

class DataCollector:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})

    def fetch_ohlcv(self, symbol, timeframe, limit):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return pd.DataFrame()