import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class AssetRanker:
    """
    Класс для финального ранжирования и сведения сигналов (Long vs Short).
    """
    
    @staticmethod
    def create_combined_ranking(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
        """
        Создает единую таблицу, сопоставляя сигналы на покупку и продажу.
        Использует Net Score = Long_Score - Short_Score.
        """
        if long_df.empty or short_df.empty:
            logger.warning("Один из DataFrame пуст. Невозможно создать комбинированный рейтинг.")
            return pd.DataFrame()

        # 1. Объединяем таблицы по coin_id (Inner Join - берем только те, что есть в обоих списках)
        # Используем суффиксы для конфликтующих колонок
        merged = pd.merge(
            long_df[['coin_id', 'symbol', 'score', 'rank']],
            short_df[['coin_id', 'score', 'rank']],
            on='coin_id',
            suffixes=('_long', '_short'),
            how='inner'
        )
        
        # 2. Расчет чистого балла (Net Score)
        # Диапазон результата: от -100 (Strong Sell) до +100 (Strong Buy)
        merged['net_score'] = merged['score_long'] - merged['score_short']
        
        # 3. Расчет расхождения рангов
        # (Rank 1 - это лучший).
        # Если LongRank=1, ShortRank=100 -> Diff = 99 (Отлично для лонга)
        # Если LongRank=100, ShortRank=1 -> Diff = -99 (Отлично для шорта)
        merged['rank_diff'] = merged['rank_short'] - merged['rank_long']
        
        # 4. Текстовая интерпретация (Signal Strength)
        conditions = [
            (merged['net_score'] >= 50),   # Лонг доминирует
            (merged['net_score'] >= 15),   # Слабый лонг
            (merged['net_score'] <= -50),  # Шорт доминирует
            (merged['net_score'] <= -15)   # Слабый шорт
        ]
        choices = ['Strong Buy', 'Buy', 'Strong Sell', 'Sell']
        
        merged['signal'] = np.select(conditions, choices, default='Neutral')
        
        # 5. Финальное ранжирование по Net Score (от самого позитивного к негативному)
        merged = merged.sort_values('net_score', ascending=False).reset_index(drop=True)
        merged['final_rank'] = merged.index + 1
        
        logger.info(f"Сформирован единый рейтинг для {len(merged)} активов.")
        return merged
    
    @staticmethod
    def analyze_signal_confluence(combined_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Анализ схождения и расхождения сигналов.
        Ищет "Верняки" (Confluence) и "Конфликты" (Conflict).
        """
        results = {}
        
        # 1. Лучшие идеи (Confluence)
        # Лонг говорит "Да" (High Score), Шорт говорит "Нет" (Low Score)
        # Сортируем по net_score
        results['top_longs'] = combined_df.head(5)[['symbol', 'net_score', 'signal']]
        results['top_shorts'] = combined_df.tail(5).sort_values('net_score', ascending=True)[['symbol', 'net_score', 'signal']]
        
        # 2. Конфликтные активы (High Volatility Zone)
        # И Лонг высоко (хороший тренд), И Шорт высоко (перекупленность/риск)
        # score_long > 60 И score_short > 60
        conflicts = combined_df[
            (combined_df['score_long'] > 60) & 
            (combined_df['score_short'] > 60)
        ].copy()
        
        conflicts['conflict_intensity'] = conflicts['score_long'] + conflicts['score_short']
        results['conflicts'] = conflicts.sort_values('conflict_intensity', ascending=False).head(5)
        
        return results

    @staticmethod
    def get_final_report_data(combined_df: pd.DataFrame) -> str:
        """Формирует текстовый блок для отчета"""
        if combined_df.empty:
            return "Нет данных для рейтинга."
            
        top_buy = combined_df.head(5)
        top_sell = combined_df.tail(5).sort_values('net_score', ascending=True)
        
        report = []
        report.append("\n⚖️ ИТОГОВЫЙ РЕЙТИНГ (Net Score):")
        report.append("-" * 40)
        report.append(f"{'Symbol':<8} {'Net Score':<10} {'Signal':<12}")
        
        # Buy Side
        for _, row in top_buy.iterrows():
            report.append(f"{row['symbol']:<8} {row['net_score']:<10.1f} {row['signal']:<12}")
            
        report.append("-" * 40)
        
        # Sell Side
        for _, row in top_sell.iterrows():
            report.append(f"{row['symbol']:<8} {row['net_score']:<10.1f} {row['signal']:<12}")
            
        return "\n".join(report)