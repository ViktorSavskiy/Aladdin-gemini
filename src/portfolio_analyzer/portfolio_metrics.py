import pandas as pd
import numpy as np

class PortfolioMetrics:
    """Расчет метрик здоровья текущего портфеля"""
    
    @staticmethod
    def calculate_portfolio_stats(portfolio_df: pd.DataFrame, 
                                scores_df: pd.DataFrame) -> dict:
        """
        Считает средний Score портфеля, диверсификацию и риск.
        """
        if portfolio_df.empty:
            return {'status': 'Empty (Cash only)'}
            
        stats = {}
        
        # 1. Total Value
        stats['total_value_usd'] = portfolio_df['value_usd'].sum()
        
        # 2. Количество активов
        stats['asset_count'] = len(portfolio_df[~portfolio_df['is_cash']])
        
        # 3. Средневзвешенный балл Aladdin Score
        # Мержим портфель с рейтингом
        merged = pd.merge(portfolio_df, scores_df[['coin_id', 'net_score']], on='coin_id', how='left')
        
        # Заполняем пропуски (если актив в портфеле есть, а рейтинга нет - считаем 0)
        merged['net_score'] = merged['net_score'].fillna(0)
        
        # Weighted Score = Sum(Weight * Score)
        # Исключаем USDT из расчета качества
        risky_assets = merged[~merged['is_cash']].copy()
        
        if not risky_assets.empty:
            # Пересчитываем веса внутри рисковой части
            risky_assets['rel_weight'] = risky_assets['value_usd'] / risky_assets['value_usd'].sum()
            
            avg_score = (risky_assets['net_score'] * risky_assets['rel_weight']).sum()
            stats['aladdin_health_score'] = avg_score
        else:
            stats['aladdin_health_score'] = 0.0
            
        return stats