import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class AssetRanker:
    """
    Класс для финального ранжирования.
    """
    
    @staticmethod
    def create_combined_ranking(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
        """Создает единую таблицу Net Score"""
        if long_df.empty or short_df.empty:
            return pd.DataFrame()

        # --- ИСПРАВЛЕНИЕ: Добавлен primary_driver в список колонок ---
        cols_long = ['coin_id', 'symbol', 'score', 'rank', 'primary_driver']
        # Проверяем, есть ли primary_driver в long_df
        if 'primary_driver' not in long_df.columns:
            long_df['primary_driver'] = 'N/A'
            
        # Объединяем
        merged = pd.merge(
            long_df[cols_long],
            short_df[['coin_id', 'score', 'rank']],
            on='coin_id',
            suffixes=('_long', '_short'),
            how='inner'
        )
        
        merged['net_score'] = merged['score_long'] - merged['score_short']
        merged['rank_diff'] = merged['rank_short'] - merged['rank_long']
        
        conditions = [
            (merged['net_score'] >= 50),
            (merged['net_score'] >= 15),
            (merged['net_score'] <= -50),
            (merged['net_score'] <= -15)
        ]
        choices = ['Strong Buy', 'Buy', 'Strong Sell', 'Sell']
        merged['signal'] = np.select(conditions, choices, default='Neutral')
        
        merged = merged.sort_values('net_score', ascending=False).reset_index(drop=True)
        merged['final_rank'] = merged.index + 1
        
        return merged
    
    @staticmethod
    def get_final_report_data(combined_df: pd.DataFrame) -> str:
        """Текстовый блок для отчета"""
        if combined_df.empty: return "Нет данных."
            
        top_buy = combined_df.head(5)
        
        report = ["\n⚖️ ИТОГОВЫЙ РЕЙТИНГ (Net Score):", "-" * 40]
        report.append(f"{'Symbol':<8} {'Net':<6} {'Signal':<10} {'Driver'}")
        
        for _, row in top_buy.iterrows():
            driver = row.get('primary_driver', '-')
            report.append(f"{row['symbol']:<8} {row['net_score']:<6.0f} {row['signal']:<10} {driver}")
            
        return "\n".join(report)