import pandas as pd
import numpy as np
from typing import Dict, Tuple, List, Optional
import logging

from config.settings import Config
from src.utils.logger import logger

class ScoreCalculator:
    """
    Класс для расчета итоговых баллов (Scoring).
    Превращает набор Z-score факторов в единую оценку 0-100.
    """
    
    def __init__(self, strategy_loader):
        self.strategy_loader = strategy_loader
    
    def calculate_scores(self, factors_df: pd.DataFrame, 
                        strategy_name: str = 'balanced') -> pd.DataFrame:
        """
        Расчет взвешенного балла по выбранной стратегии.
        """
        logger.info(f"⚖️ Расчет баллов (Стратегия: {strategy_name})...")
        
        if factors_df.empty:
            logger.warning("Нет данных факторов для расчета.")
            return pd.DataFrame()

        # 1. Получаем веса стратегии
        strategy = self.strategy_loader.get_strategy(strategy_name)
        weights = strategy.get('weights', {})
        
        if not weights:
            logger.error(f"В стратегии {strategy_name} нет весов!")
            return pd.DataFrame()
        
        # 2. Подготовка DataFrame
        scores_df = factors_df[['coin_id', 'symbol']].copy()
        
        # Сырой балл (сумма взвешенных Z-scores)
        # Z-scores обычно от -3 до 3. Сумма может быть от -3 до 3 (т.к. веса в сумме 1.0).
        raw_score = np.zeros(len(factors_df))
        
        used_factors = []
        
        for factor, weight in weights.items():
            if factor in factors_df.columns:
                # Основная формула скоринга: Score += Factor_Value * Weight
                raw_score += factors_df[factor].values * weight
                used_factors.append(factor)
            else:
                logger.debug(f"Фактор '{factor}' отсутствует в данных (считаем за 0).")
        
        scores_df['raw_score'] = raw_score
        
        # 3. Нормализация (0-100)
        # Используем сигмоиду или MinMax.
        # MinMax лучше для ранжирования текущего списка.
        min_s = raw_score.min()
        max_s = raw_score.max()
        
        if max_s > min_s:
            # Масштабируем от 0 до 100
            scores_df['score'] = ((raw_score - min_s) / (max_s - min_s)) * 100
        else:
            scores_df['score'] = 50.0 # Если все равны
            
        # 4. Добавляем человекочитаемую категорию
        scores_df['rank'] = scores_df['score'].rank(ascending=False, method='min').astype(int)
        
        scores_df['verdict'] = pd.cut(
            scores_df['score'],
            bins=[-1, 20, 40, 60, 80, 101],
            labels=['Strong Sell', 'Sell', 'Neutral', 'Buy', 'Strong Buy']
        )
        
        # 5. Анализ вклада (Contribution Analysis) - Почему такой балл?
        # Находим фактор, который внес наибольший вклад в оценку
        # Это полезно для отладки: "Почему PEPE топ-1? А, из-за momentum_30d"
        top_factors = []
        for idx in factors_df.index:
            # Считаем вклад каждого фактора: value * weight
            contributions = {f: factors_df.loc[idx, f] * weights[f] for f in used_factors}
            # Находим макс
            best_factor = max(contributions, key=contributions.get)
            top_factors.append(best_factor)
            
        scores_df['primary_driver'] = top_factors

        # Сортировка
        scores_df = scores_df.sort_values('score', ascending=False).reset_index(drop=True)
        
        top_asset = scores_df.iloc[0]
        logger.info(f"Лидер рейтинга: {top_asset['symbol']} (Score: {top_asset['score']:.1f}, Driver: {top_asset['primary_driver']})")
        
        return scores_df
    
    def calculate_dual_scores(self, factors_df: pd.DataFrame, 
                            long_strat: str = 'balanced',
                            short_strat: str = 'short_speculative') -> Dict[str, pd.DataFrame]:
        """
        Расчет сразу двух таблиц: для Лонга и для Шорта.
        Использует обновленные имена стратегий из StrategyLoader.
        """
        results = {}
        
        # 1. Long Score
        results['long'] = self.calculate_scores(factors_df, strategy_name=long_strat)
        
        # 2. Short Score
        # Для шорта мы используем отдельную стратегию, где веса настроены на поиск падающих активов
        results['short'] = self.calculate_scores(factors_df, strategy_name=short_strat)
        
        return results
    
    def get_top_assets(self, scores_df: pd.DataFrame, top_n: int = 10, min_score: float = 60.0) -> pd.DataFrame:
        """
        Возвращает Топ-N активов, которые выше проходного балла.
        """
        if scores_df.empty:
            return pd.DataFrame()
            
        filtered = scores_df[scores_df['score'] >= min_score].copy()
        
        # Оставляем только нужные колонки для отчета
        cols = ['rank', 'symbol', 'score', 'verdict', 'primary_driver']
        # Если есть другие полезные колонки, оставляем их
        final_cols = [c for c in cols if c in filtered.columns]
        
        return filtered[final_cols].head(top_n)