# src/scoring_engine/factor_calculator.py

import pandas as pd
import numpy as np
from typing import Dict, List

class FactorCalculator:
    # ... (старые методы статических расчетов оставляем) ...

    @staticmethod
    def prepare_price_matrix(historical_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Преобразует словарь с историей в одну большую таблицу цен.
        Index: Date, Columns: Coin_ID, Values: Price
        """
        df_list = []
        for coin_id, df in historical_data.items():
            if df.empty: continue
            temp = df[['date', 'price']].copy()
            temp['date'] = pd.to_datetime(temp['date'])
            temp = temp.set_index('date')
            temp.columns = [coin_id]
            df_list.append(temp)
        
        if not df_list: return pd.DataFrame()
        
        # Объединяем (Outer Join), сортируем и заполняем пропуски (ffill)
        price_matrix = pd.concat(df_list, axis=1).sort_index().ffill()
        return price_matrix

    @staticmethod
    def calculate_rolling_factors(price_matrix: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Расчет факторов для КАЖДОГО дня в истории.
        Возвращает словарь матриц факторов.
        """
        factors = {}
        
        # 1. Momentum (30d Return)
        # pct_change(30) считает доходность за 30 дней для каждой точки времени
        factors['momentum_30d'] = price_matrix.pct_change(30)
        
        # 2. Volatility (30d Rolling Std)
        # log returns для корректности
        log_ret = np.log(price_matrix / price_matrix.shift(1))
        # std * sqrt(365)
        factors['volatility_30d'] = log_ret.rolling(window=30).std() * np.sqrt(365)
        
        # 3. Reversal / Short Momentum (7d)
        factors['momentum_7d'] = price_matrix.pct_change(7)
        
        # 4. Quality (Rolling Sharpe)
        # Return / Risk
        factors['quality_sharpe'] = factors['momentum_30d'] / factors['volatility_30d'].replace(0, np.nan)
        
        # Нормализация (Z-Score Cross-Sectional)
        # Для каждого дня мы сравниваем монеты между собой
        normalized_factors = {}
        
        for name, df in factors.items():
            # Вычитаем среднее по строке (axis=1), делим на std по строке
            mean = df.mean(axis=1)
            std = df.std(axis=1)
            
            # Z-Score = (X - Mean) / Std
            zscore = df.sub(mean, axis=0).div(std, axis=0)
            
            # Клиппинг и Инверсия
            zscore = zscore.clip(-3, 3)
            
            if name == 'volatility_30d': # Низкая волатильность = хорошо
                zscore = -zscore
            if name == 'momentum_7d': # Для Reversal стратегии
                zscore = -zscore 
                
            normalized_factors[name] = zscore.fillna(0) # Заполняем пропуски нулями (нейтрально)
            
        return normalized_factors