import pandas as pd
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
import ccxt

from config.settings import Config
from src.utils.logger import logger

class DataFetcher:
    """Класс для получения данных с различных API с обработкой ошибок и лимитов"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CryptoAladdin/1.0',
            'Accept': 'application/json'
        })
        
        # Настройка CoinGecko
        self.cg_base_url = "https://api.coingecko.com/api/v3"
        api_key = Config.COINGECKO_API_KEY
        
        if api_key:
            self.session.headers.update({'x-cg-demo-api-key': api_key})
            logger.info("CoinGecko API Key найден и применен.")
        
        self.cg_rate_limit = 60 / Config.API_RATE_LIMITS.get('coingecko', 5)
        
        # Инициализация Binance
        self.binance = None
        if Config.DATA_SOURCES.get("binance"):
            try:
                exchange_config = {'enableRateLimit': True}
                if hasattr(Config, 'BINANCE_API_KEY') and Config.BINANCE_API_KEY:
                    exchange_config['apiKey'] = Config.BINANCE_API_KEY
                    exchange_config['secret'] = Config.BINANCE_API_SECRET
                
                self.binance = ccxt.binance(exchange_config)
            except Exception as e:
                logger.error(f"Ошибка инициализации Binance: {e}")

    def _make_request(self, url: str, params: Dict, retries: int = 3) -> Optional[Dict]:
        """Внутренний метод для запросов с повторными попытками"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    wait_time = (attempt + 2) * 5
                    logger.warning(f"Превышен лимит API (429). Ждем {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка запроса (попытка {attempt+1}/{retries}): {e}")
                time.sleep(2)
        
        return None

    def fetch_coingecko_market_data(self, pages: int = 4) -> pd.DataFrame:
        """Получение данных о рынке (Top N монет)"""
        logger.info("Сбор рыночных данных с CoinGecko...")
        
        url = f"{self.cg_base_url}/coins/markets"
        base_params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "sparkline": "false",
            "price_change_percentage": "24h,7d,30d"
        }
        
        all_data = []
        
        for page in range(1, pages + 1):
            base_params["page"] = page
            data = self._make_request(url, base_params)
            
            if not data:
                logger.warning(f"Не удалось получить данные для страницы {page}")
                break
                
            all_data.extend(data)
            time.sleep(self.cg_rate_limit) 
        
        if not all_data:
            return pd.DataFrame()

        # Обработка данных
        try:
            df = pd.DataFrame(all_data)
            
            # Маппинг колонок
            column_mapping = {
                'id': 'coin_id',
                'symbol': 'symbol',
                'name': 'name',
                'current_price': 'price',
                'market_cap': 'market_cap',
                'total_volume': 'volume_24h',
                # CoinGecko может вернуть оба поля, и оба переименуются в change_24h
                'price_change_percentage_24h_in_currency': 'change_24h', 
                'price_change_percentage_24h': 'change_24h',
                'price_change_percentage_7d_in_currency': 'change_7d',
                'price_change_percentage_30d_in_currency': 'change_30d',
                'last_updated': 'last_updated'
            }
            
            df = df.rename(columns=column_mapping)
            
            # --- ИСПРАВЛЕНИЕ: Удаляем дубликаты колонок ---
            # Если API вернул две колонки change_24h, оставляем одну
            df = df.loc[:, ~df.columns.duplicated()]
            # ----------------------------------------------
            
            cols_to_keep = ['coin_id', 'symbol', 'name', 'price', 'market_cap', 
                           'volume_24h', 'change_24h', 'change_7d', 'change_30d']
            
            # Оставляем только те колонки, которые реально есть в датафрейме
            final_cols = [c for c in cols_to_keep if c in df.columns]
            
            df = df[final_cols].copy()
            df['timestamp'] = datetime.now()
            df['date'] = df['timestamp'].dt.date
            
            logger.info(f"Успешно загружено {len(df)} монет")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при обработке DataFrame CoinGecko: {e}")
            return pd.DataFrame()

    def fetch_historical_data(self, coin_id: str, days: int = 90) -> pd.DataFrame:
        """Получение истории для одной монеты"""
        url = f"{self.cg_base_url}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily"
        }
        
        data = self._make_request(url, params)
        if not data:
            return pd.DataFrame()
            
        try:
            prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            prices['date'] = pd.to_datetime(prices['timestamp'], unit='ms').dt.date
            
            if 'total_volumes' in data:
                volumes = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
                volumes['date'] = pd.to_datetime(volumes['timestamp'], unit='ms').dt.date
                prices = pd.merge(prices, volumes[['date', 'volume']], on='date', how='left')
            
            prices['coin_id'] = coin_id
            prices = prices.drop('timestamp', axis=1)
            
            time.sleep(self.cg_rate_limit)
            return prices
            
        except Exception as e:
            logger.error(f"Ошибка парсинга истории для {coin_id}: {e}")
            return pd.DataFrame()

    def fetch_all_historical_data(self, coin_ids: List[str], days: int = 90) -> Dict[str, pd.DataFrame]:
        """Сбор истории для списка монет"""
        historical_data = {}
        total = len(coin_ids)
        logger.info(f"Начинаем сбор истории для {total} монет. Дней: {days}")
        
        for i, coin_id in enumerate(coin_ids, 1):
            if i % 10 == 0:
                logger.info(f"Прогресс: {i}/{total}...")
            
            df = self.fetch_historical_data(coin_id, days)
            if not df.empty:
                historical_data[coin_id] = df
            
            if i % 50 == 0:
                time.sleep(5)
                
        return historical_data