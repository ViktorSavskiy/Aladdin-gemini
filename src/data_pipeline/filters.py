import pandas as pd
from typing import List, Optional
from config.settings import Config  # Обновленный импорт
from src.utils.logger import logger

class DataFilter:
    """Класс для фильтрации и категоризации криптовалютных данных"""
    
    # Расширенный список стейблкоинов для фильтрации
    STABLECOINS = {
        'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP', 'USD', 
        'FDUSD', 'PYUSD', 'USDE', 'GUSD', 'LUSD', 'FRAX'
    }

    @staticmethod
    def filter_by_market_cap(df: pd.DataFrame, min_cap: float = None) -> pd.DataFrame:
        """Фильтрация по минимальной рыночной капитализации"""
        if df.empty:
            return df

        if min_cap is None:
            min_cap = Config.MIN_MARKET_CAP
        
        if 'market_cap' not in df.columns:
            logger.warning("Колонка market_cap отсутствует. Пропуск фильтра.")
            return df
        
        # Заполняем NaN нулями, чтобы избежать ошибок сравнения
        initial_count = len(df)
        df_clean = df.fillna({'market_cap': 0})
        filtered = df_clean[df_clean['market_cap'] >= min_cap].copy()
        
        if len(filtered) < initial_count:
            logger.info(f"Фильтр MarketCap (${min_cap:,.0f}): {initial_count} -> {len(filtered)}")
            
        return filtered
    
    @staticmethod
    def filter_by_volume(df: pd.DataFrame, min_volume: float = None) -> pd.DataFrame:
        """Фильтрация по минимальному объему торгов"""
        if df.empty:
            return df

        if min_volume is None:
            min_volume = Config.MIN_VOLUME_24H
        
        if 'volume_24h' not in df.columns:
            logger.warning("Колонка volume_24h отсутствует. Пропуск фильтра.")
            return df
        
        initial_count = len(df)
        df_clean = df.fillna({'volume_24h': 0})
        filtered = df_clean[df_clean['volume_24h'] >= min_volume].copy()
        
        if len(filtered) < initial_count:
            logger.info(f"Фильтр Volume (${min_volume:,.0f}): {initial_count} -> {len(filtered)}")
            
        return filtered
    
    @staticmethod
    def filter_by_price(df: pd.DataFrame, min_price: float = 0.00000001) -> pd.DataFrame:
        """
        Фильтрация по минимальной цене.
        min_price по умолчанию очень маленький, чтобы не отсечь мем-коины.
        """
        if df.empty or 'price' not in df.columns:
            return df
        
        filtered = df[df['price'] >= min_price].copy()
        return filtered

    @staticmethod
    def remove_stablecoins(df: pd.DataFrame) -> pd.DataFrame:
        """Удаляет стейблкоины из выборки (полезно для анализа волатильности)"""
        if df.empty or 'symbol' not in df.columns:
            return df
            
        initial_count = len(df)
        # Проверяем вхождение в список STABLECOINS (приводим к верхнему регистру)
        mask = ~df['symbol'].str.upper().isin(DataFilter.STABLECOINS)
        filtered = df[mask].copy()
        
        removed = initial_count - len(filtered)
        if removed > 0:
            logger.info(f"Удалено стейблкоинов: {removed}")
            
        return filtered
    
    @staticmethod
    def categorize_assets(df: pd.DataFrame) -> pd.DataFrame:
        """Категоризация активов (BTC, ETH, Stable, Altcoin)"""
        if df.empty:
            return df
        
        df = df.copy()
        df['category'] = 'altcoin' # Значение по умолчанию
        
        # Приводим символы к верхнему регистру один раз для проверок
        symbols = df['symbol'].str.upper()
        
        # Определяем BTC
        df.loc[symbols.isin(['BTC', 'WBTC', 'BITCOIN']), 'category'] = 'bitcoin'
        
        # Определяем ETH
        df.loc[symbols.isin(['ETH', 'WETH', 'ETHEREUM', 'STETH']), 'category'] = 'ethereum'
        
        # Определяем стейблкоины (используем константу класса)
        df.loc[symbols.isin(DataFilter.STABLECOINS), 'category'] = 'stablecoin'
        
        # Логируем статистику
        stats = df['category'].value_counts().to_dict()
        logger.info(f"Категории активов: {stats}")
        
        return df

    @staticmethod
    def apply_all_filters(df: pd.DataFrame, exclude_stables: bool = True) -> pd.DataFrame:
        """
        Применение полного пайплайна фильтрации.
        Args:
            df: Исходный DataFrame
            exclude_stables: Если True, удаляет стейблкоины (рекомендуется для анализа)
        """
        if df.empty:
            return df
        
        logger.info("--- Начало фильтрации данных ---")
        
        # 1. Удаляем полные дубликаты символов (оставляем с большей капитализацией, если есть коллизии)
        # Сортируем, чтобы при drop_duplicates остался самый крупный актив с таким тикером
        if 'market_cap' in df.columns:
            df = df.sort_values('market_cap', ascending=False)
        
        if 'symbol' in df.columns:
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['symbol'], keep='first')
            if len(df) < before_dedup:
                logger.info(f"Удалено дубликатов тикеров: {before_dedup - len(df)}")

        # 2. Основные фильтры
        df = DataFilter.filter_by_market_cap(df)
        df = DataFilter.filter_by_volume(df)
        
        # 3. Фильтр цены (очень мягкий)
        df = DataFilter.filter_by_price(df)
        
        # 4. Категоризация
        df = DataFilter.categorize_assets(df)
        
        # 5. Опционально убираем стейблы
        if exclude_stables:
            df = DataFilter.remove_stablecoins(df)
            
        logger.info(f"--- Фильтрация завершена. Осталось активов: {len(df)} ---")
        
        return df