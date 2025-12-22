import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging

from config.settings import Config
from src.utils.logger import logger

class PortfolioComparator:
    """Сравнение текущего и целевого портфеля"""
    
    def __init__(self):
        self.config = Config.PORTFOLIO_CONFIG

    def calculate_target_portfolio(self, ranking_df: pd.DataFrame, 
                                 total_portfolio_value: float) -> pd.DataFrame:
        """
        Превращает рейтинг (Score) в целевые веса (Target Weights).
        Используем метод: Вес пропорционален баллу (Score-based Allocation).
        """
        # Берем топ-N активов из рейтинга (например, топ-10)
        top_n = self.config.get('max_assets', 10)
        
        # Фильтруем только Strong Buy / Buy
        candidates = ranking_df[
            (ranking_df['net_score'] > 20)  # Только позитивные
        ].head(top_n).copy()
        
        if candidates.empty:
            logger.warning("Нет хороших активов для покупки! Рекомендуется выйти в кэш.")
            return pd.DataFrame()

        # Нормализуем баллы, чтобы сумма весов была 1.0 (или 0.95, оставляя 5% в кэше)
        # Формула: Вес = Score / Sum(Scores)
        # Используем net_score или long_score
        score_sum = candidates['net_score'].sum()
        
        if score_sum > 0:
            candidates['target_weight'] = candidates['net_score'] / score_sum
        else:
            # Если что-то пошло не так, равные веса
            candidates['target_weight'] = 1.0 / len(candidates)
            
        # Рассчитываем целевую сумму в долларах
        # Оставляем 5% в USDT на всякий случай
        target_equity = total_portfolio_value * 0.95
        candidates['target_value_usd'] = candidates['target_weight'] * target_equity
        
        return candidates[['coin_id', 'symbol', 'net_score', 'target_weight', 'target_value_usd']]

    def compare_portfolios(self, current_portfolio: pd.DataFrame, 
                         target_portfolio: pd.DataFrame) -> pd.DataFrame:
        """
        Сравнивает текущий и целевой портфели, вычисляет отклонения (Delta).
        """
        logger.info("⚖️ Сравнение портфелей (Plan vs Fact)...")
        
        if current_portfolio.empty:
            # Если портфель пуст (мы в кэше), то текущие веса = 0
            current_portfolio = pd.DataFrame(columns=['coin_id', 'symbol', 'value_usd', 'current_weight'])
            total_value = 1000.0 # Виртуальная сумма для старта, если реальной нет
        else:
            total_value = current_portfolio['value_usd'].sum()

        # Если target_portfolio еще не имеет target_value_usd (если мы пришли с пустым портфелем)
        if 'target_value_usd' not in target_portfolio.columns:
             target_portfolio = self.calculate_target_portfolio(target_portfolio, total_value)

        # Объединяем (Full Outer Join), чтобы видеть:
        # 1. Что нужно купить (есть в Target, нет в Current)
        # 2. Что нужно продать (есть в Current, нет в Target)
        # 3. Что нужно ребалансировать (есть и там, и там)
        
        merged = pd.merge(
            target_portfolio[['coin_id', 'symbol', 'target_weight', 'target_value_usd']],
            current_portfolio[['coin_id', 'value_usd', 'current_weight', 'amount', 'current_price']],
            on='coin_id',
            how='outer',
            suffixes=('_tgt', '_cur')
        )
        
        # Заполняем пропуски
        merged['symbol'] = merged['symbol_tgt'].combine_first(merged['symbol_cur'])
        merged[['target_weight', 'target_value_usd', 'value_usd', 'current_weight']] = \
            merged[['target_weight', 'target_value_usd', 'value_usd', 'current_weight']].fillna(0)
            
        # Расчет отклонений (Delta)
        merged['weight_delta'] = merged['target_weight'] - merged['current_weight']
        merged['value_delta'] = merged['target_value_usd'] - merged['value_usd']
        
        # Добавляем действие (Action)
        # Используем порог из конфига
        threshold = self.config.get('rebalance_threshold_pct', 0.05)
        
        conditions = [
            (merged['weight_delta'] > threshold),  # Нужно докупить
            (merged['weight_delta'] < -threshold)  # Нужно продать
        ]
        choices = ['BUY', 'SELL']
        merged['action'] = np.select(conditions, choices, default='HOLD')
        
        # Сортируем: сначала продажи (чтобы освободить кэш), потом покупки
        merged = merged.sort_values('value_delta', ascending=True)
        
        return merged