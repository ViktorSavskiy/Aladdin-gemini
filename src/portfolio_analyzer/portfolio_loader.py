import pandas as pd
import ccxt
from typing import Dict, List
import logging

from config.settings import Config
from src.utils.logger import logger

class PortfolioLoader:
    """Загрузка текущего состояния портфеля с Bybit"""
    
    def __init__(self):
        self.config = Config.PORTFOLIO_CONFIG
        self.exchange = None
        
        # Инициализация Bybit
        if self.config['source'] == 'bybit':
            try:
                # Проверяем наличие ключей
                api_key = getattr(Config, 'BYBIT_API_KEY', None)
                secret = getattr(Config, 'BYBIT_API_SECRET', None)
                
                if not api_key or not secret:
                    logger.error("❌ Не найдены API ключи Bybit в Config!")
                    return

                self.exchange = ccxt.bybit({
                    'apiKey': api_key,
                    'secret': secret,
                    'enableRateLimit': True,
                    # Опции для Bybit (важно для Unified Account)
                    'options': {
                        'defaultType': 'spot', 
                        'adjustForTimeDifference': True
                    }
                })
                # Подгружаем рынки, чтобы знать тикеры (BTC/USDT и т.д.)
                self.exchange.load_markets()
                
            except Exception as e:
                logger.error(f"Ошибка подключения к Bybit: {e}")

    def load_portfolio(self, current_prices: pd.DataFrame) -> pd.DataFrame:
        """
        Возвращает DataFrame с текущими активами.
        """
        logger.info("💼 Запрос баланса с Bybit...")
        
        holdings = {}
        
        # 1. Получаем баланс
        if self.exchange:
            try:
                # fetch_balance на Bybit возвращает сложную структуру
                # ccxt унифицирует это в поле 'total'
                balance = self.exchange.fetch_balance()
                
                # Берем только те монеты, где баланс > 0
                if 'total' in balance:
                    holdings = {k: v for k, v in balance['total'].items() if v > 0}
                else:
                    logger.warning("Структура баланса Bybit пуста (возможно, неверные права ключа).")
                    
            except Exception as e:
                logger.error(f"Не удалось получить баланс Bybit: {e}")
                return pd.DataFrame()
        else:
            # Fallback для тестов (если source='manual')
            holdings = {
                'USDT': 1000.0, # Пример
                'BTC': 0.0 
            }
        
        if not holdings:
            logger.warning("Портфель пуст или ошибка доступа.")
            return pd.DataFrame()

        # 2. Обогащаем данными о ценах из нашего Модуля 1
        portfolio_list = []
        base_currency = self.config['base_currency'] # USDT
        
        # Подготовка маппингов (Символ -> Цена, Символ -> ID)
        # Приводим символы к верхнему регистру для надежности
        price_map = dict(zip(current_prices['symbol'].str.upper(), current_prices['price']))
        id_map = dict(zip(current_prices['symbol'].str.upper(), current_prices['coin_id']))

        for symbol, amount in holdings.items():
            symbol = symbol.upper()
            
            # Игнорируем мелкую пыль (меньше 0.000001 монеты), кроме USDT
            if symbol != base_currency and amount < 1e-6:
                continue

            # Цена
            if symbol == base_currency:
                price = 1.0
                coin_id = 'tether'
            else:
                price = price_map.get(symbol, 0.0)
                coin_id = id_map.get(symbol, None)
                
                # Если CoinGecko не знает такую монету (например, какой-то эйрдроп на бирже)
                if not coin_id or price == 0:
                    # Попробуем получить цену прямо с биржи через ccxt, если её нет в нашей базе
                    try:
                        ticker = self.exchange.fetch_ticker(f"{symbol}/USDT")
                        price = ticker['last']
                        coin_id = f"bybit_{symbol.lower()}" # Временный ID
                    except:
                        logger.warning(f"Неизвестный актив на балансе: {symbol}, пропускаем.")
                        continue

            value_usd = amount * price
            
            # Фильтруем "пыль" по стоимости (меньше $1)
            if value_usd > 1.0:
                portfolio_list.append({
                    'coin_id': coin_id,
                    'symbol': symbol,
                    'amount': amount,
                    'current_price': price,
                    'value_usd': value_usd,
                    'is_cash': (symbol == base_currency)
                })
        
        df = pd.DataFrame(portfolio_list)
        
        # 3. Считаем доли
        if not df.empty:
            total_value = df['value_usd'].sum()
            df['current_weight'] = df['value_usd'] / total_value
            
            logger.info(f"✅ Баланс Bybit загружен. Активов: {len(df)}. Total: ${total_value:.2f}")
            return df
        
        return pd.DataFrame()