import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats.mstats import winsorize
from typing import Tuple, Dict, List, Optional
import logging

from config.settings import Config
from src.utils.logger import logger

class FactorCalculator:
    """
    Класс для расчета факторов (Z-scores).
    Включает:
    1. Статический расчет (для текущего скоринга).
    2. Скользящий расчет (Rolling) для бэктеста.
    """
    
    @staticmethod
    def _winsorize_series(series: pd.Series, limits=(0.01, 0.01)) -> pd.Series:
        """Обрезает экстремальные значения"""
        if series.empty: return series
        clean_series = series.fillna(series.median())
        try:
            return pd.Series(winsorize(clean_series, limits=limits), index=series.index)
        except:
            return clean_series

    @staticmethod
    def calculate_zscore_factor(series: pd.Series, reverse: bool = False, clip_range: float = 3.0) -> pd.Series:
        """Расчет Z-score"""
        if series.empty or series.isnull().all():
            return pd.Series(0.0, index=series.index)
        
        series_win = FactorCalculator._winsorize_series(series)
        zscore = stats.zscore(series_win, nan_policy='omit')
        zscore = np.nan_to_num(zscore)
        zscore_series = pd.Series(zscore, index=series.index)
        
        if reverse:
            zscore_series = -zscore_series
            
        return zscore_series.clip(-clip_range, clip_range)
    
    # --- СТАТИЧЕСКИЕ ФАКТОРЫ (ДЛЯ ТЕКУЩЕГО МОМЕНТА) ---
    
    @staticmethod
    def calculate_momentum_factors(df: pd.DataFrame) -> Dict[str, pd.Series]:
        factors = {}
        if 'return_30d' in df.columns:
            factors['momentum_30d'] = FactorCalculator.calculate_zscore_factor(df['return_30d'])
        if 'return_7d' in df.columns:
            factors['momentum_7d_bearish'] = FactorCalculator.calculate_zscore_factor(df['return_7d'], reverse=True)
        return factors
    
    @staticmethod
    def calculate_volatility_factors(df: pd.DataFrame) -> Dict[str, pd.Series]:
        factors = {}
        if 'volatility_30d' in df.columns:
            factors['low_volatility'] = FactorCalculator.calculate_zscore_factor(df['volatility_30d'], reverse=True)
            factors['high_volatility'] = FactorCalculator.calculate_zscore_factor(df['volatility_30d'])
        return factors
    
    @staticmethod
    def calculate_value_size_factors(df: pd.DataFrame) -> Dict[str, pd.Series]:
        factors = {}
        if 'market_cap' in df.columns:
            factors['size_large'] = FactorCalculator.calculate_zscore_factor(np.log1p(df['market_cap']))
        if 'market_cap' in df.columns and 'transaction_volume' in df.columns:
            vol = df['transaction_volume'].replace(0, np.nan)
            nvt = df['market_cap'] / vol
            factors['value_nvt'] = FactorCalculator.calculate_zscore_factor(np.log1p(nvt), reverse=True)
        return factors
    
    @staticmethod
    def calculate_quality_factors(df: pd.DataFrame) -> Dict[str, pd.Series]:
        factors = {}
        if 'sharpe_90d' in df.columns:
            factors['quality_sharpe'] = FactorCalculator.calculate_zscore_factor(df['sharpe_90d'])
        if 'developer_score' in df.columns:
            factors['quality_dev'] = FactorCalculator.calculate_zscore_factor(df['developer_score'])
        return factors

    @staticmethod
    def calculate_category_factors(metrics_df: pd.DataFrame, category_df: pd.DataFrame) -> Dict[str, pd.Series]:
        factors = {}
        if category_df.empty: return factors
        
        merged_df = pd.merge(metrics_df[['coin_id']], category_df, on='coin_id', how='left')
        merged_df.index = metrics_df.index 

        category_weights = {'DeFi': 1.1, 'L1': 1.0, 'L2': 1.2, 'Meme': 0.6, 'Gaming': 0.8, 'NFT': 0.7}
        
        if 'category' in merged_df.columns:
            cat_weight = merged_df['category'].map(category_weights).fillna(1.0)
            factors['category_advantage'] = FactorCalculator.calculate_zscore_factor(cat_weight)

        if 'tvl' in merged_df.columns:
            tvl_log = np.log1p(merged_df['tvl'].fillna(0))
            factors['tvl_strength'] = FactorCalculator.calculate_zscore_factor(tvl_log)

        if 'tvl_ratio' in merged_df.columns:
            ratio = merged_df['tvl_ratio'].replace(0, np.nan)
            factors['defi_value'] = FactorCalculator.calculate_zscore_factor(np.log1p(ratio), reverse=True)

        return factors
    
    @staticmethod
    def calculate_all_factors(metrics_df: pd.DataFrame, category_df: pd.DataFrame = None) -> pd.DataFrame:
        """Главный метод для текущего скоринга"""
        logger.info("🧮 Расчет факторов оценки (Z-scores)...")
        if metrics_df.empty: return pd.DataFrame()

        factors_df = metrics_df[['coin_id', 'symbol']].copy()
        all_factors = {}
        
        calculators = [
            FactorCalculator.calculate_momentum_factors,
            FactorCalculator.calculate_volatility_factors,
            FactorCalculator.calculate_value_size_factors,
            FactorCalculator.calculate_quality_factors
        ]
        
        for calc in calculators:
            try:
                all_factors.update(calc(metrics_df))
            except Exception as e:
                logger.error(f"Ошибка в {calc.__name__}: {e}")
        
        if category_df is not None and not category_df.empty:
            try:
                all_factors.update(FactorCalculator.calculate_category_factors(metrics_df, category_df))
            except Exception as e:
                logger.error(f"Ошибка расчета категорийных факторов: {e}")

        for name, series in all_factors.items():
            factors_df[name] = series
            
        numeric_cols = factors_df.select_dtypes(include=[np.number]).columns
        factors_df[numeric_cols] = factors_df[numeric_cols].fillna(0.0)
        
        return factors_df

    # --- МЕТОДЫ ДЛЯ БЭКТЕСТА (ВОТ ИХ НЕ ХВАТАЛО) ---

    @staticmethod
    def prepare_price_matrix(historical_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Преобразует словарь с историей в матрицу цен (Index=Date, Col=CoinID)"""
        if not historical_data: return pd.DataFrame()
        
        df_list = []
        for coin_id, df in historical_data.items():
            if df.empty or 'price' not in df.columns: continue
            temp = df[['date', 'price']].copy()
            temp['date'] = pd.to_datetime(temp['date'])
            temp = temp.set_index('date')
            # Удаляем дубликаты дат, если есть
            temp = temp[~temp.index.duplicated(keep='first')]
            temp.columns = [coin_id]
            df_list.append(temp)
        
        if not df_list: return pd.DataFrame()
        
        # Объединяем и заполняем пропуски
        price_matrix = pd.concat(df_list, axis=1).sort_index()
        return price_matrix.ffill()

    @staticmethod
    def calculate_rolling_factors(price_matrix: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Расчет факторов для каждого дня истории"""
        factors = {}
        if price_matrix.empty: return factors
        
        # 1. Momentum (30d)
        factors['momentum_30d'] = price_matrix.pct_change(30)
        
        # 2. Volatility (30d)
        log_ret = np.log(price_matrix / price_matrix.shift(1))
        factors['low_volatility'] = -(log_ret.rolling(30).std() * np.sqrt(365)) # Инвертируем (низкая = хорошо)
        
        # 3. Reversal (7d)
        factors['momentum_7d_bearish'] = -(price_matrix.pct_change(7)) # Инвертируем (падение = хорошо для шорта)
        
        # 4. Quality (Sharpe)
        vol = log_ret.rolling(30).std() * np.sqrt(365)
        factors['quality_sharpe'] = factors['momentum_30d'] / vol.replace(0, np.nan)
        
        # Нормализация Z-score по каждому дню (Cross-sectional)
        norm_factors = {}
        for name, df in factors.items():
            # (X - Mean) / Std
            mean = df.mean(axis=1)
            std = df.std(axis=1)
            zscore = df.sub(mean, axis=0).div(std, axis=0)
            norm_factors[name] = zscore.clip(-3, 3).fillna(0)
            
        return norm_factors