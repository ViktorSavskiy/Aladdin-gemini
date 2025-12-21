import pandas as pd
import numpy as np
from typing import Dict, Optional
from src.utils.logger import logger


class MarketRegimeDetector:
    """
    Определяет текущий режим рынка (бычий/медвежий/боковик)
    и предлагает подходящую стратегию.
    """
    
    @staticmethod
    def analyze_market_condition(market_data: pd.DataFrame, 
                                 historical_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        Анализирует текущее состояние рынка.
        
        Args:
            market_data: Текущие рыночные данные
            historical_data: Словарь с историческими данными {coin_id: DataFrame}
        
        Returns:
            dict: {
                'regime': 'bull' | 'bear' | 'neutral',
                'suggested_strategy': str,
                'btc_change_7d': float,
                'btc_change_30d': float,
                'volatility': float
            }
        """
        try:
            # Получаем данные BTC
            btc_data = historical_data.get('bitcoin')
            if btc_data is None or btc_data.empty:
                # Пытаемся найти BTC в market_data
                btc_market = market_data[market_data['coin_id'] == 'bitcoin']
                if btc_market.empty:
                    logger.warning("BTC данные не найдены, используем нейтральный режим")
                    return {
                        'regime': 'neutral',
                        'suggested_strategy': 'balanced',
                        'btc_change_7d': 0.0,
                        'btc_change_30d': 0.0,
                        'volatility': 0.0
                    }
            
            # Если есть исторические данные BTC
            if btc_data is not None and not btc_data.empty:
                btc_df = btc_data.copy()
                if 'date' in btc_df.columns:
                    btc_df['date'] = pd.to_datetime(btc_df['date'])
                    btc_df = btc_df.set_index('date').sort_index()
                
                prices = btc_df['price'] if 'price' in btc_df.columns else btc_df.iloc[:, 0]
                
                if len(prices) < 30:
                    logger.warning("Недостаточно данных BTC для анализа режима")
                    return {
                        'regime': 'neutral',
                        'suggested_strategy': 'balanced',
                        'btc_change_7d': 0.0,
                        'btc_change_30d': 0.0,
                        'volatility': 0.0
                    }
                
                # Расчет изменений
                current_price = prices.iloc[-1]
                price_7d = prices.iloc[-7] if len(prices) >= 7 else prices.iloc[0]
                price_30d = prices.iloc[-30] if len(prices) >= 30 else prices.iloc[0]
                
                change_7d = (current_price / price_7d - 1) * 100 if price_7d > 0 else 0
                change_30d = (current_price / price_30d - 1) * 100 if price_30d > 0 else 0
                
                # Волатильность (30 дней)
                returns = prices.pct_change().dropna()
                volatility = returns.std() * np.sqrt(30) * 100 if len(returns) > 0 else 0
                
            else:
                # Используем данные из market_data
                btc_market = market_data[market_data['coin_id'] == 'bitcoin']
                if not btc_market.empty:
                    change_7d = btc_market.iloc[0].get('price_change_percentage_7d', 0) or 0
                    change_30d = btc_market.iloc[0].get('price_change_percentage_30d', 0) or 0
                    volatility = 0.0  # Не можем рассчитать без истории
                else:
                    change_7d = change_30d = volatility = 0.0
            
            # Определение режима
            if change_30d > 20:
                regime = 'bull'
                strategy = 'momentum_growth'
            elif change_30d < -20:
                regime = 'bear'
                strategy = 'value_defensive'
            elif change_7d > 5:
                regime = 'bull'
                strategy = 'momentum_growth'
            elif change_7d < -5:
                regime = 'bear'
                strategy = 'value_defensive'
            else:
                regime = 'neutral'
                strategy = 'balanced'
            
            logger.info(f"BTC изменение (7d/30d): {change_7d:.2f}% / {change_30d:.2f}%")
            logger.info(f"Волатильность: {volatility:.2f}%")
            
            return {
                'regime': regime,
                'suggested_strategy': strategy,
                'btc_change_7d': change_7d,
                'btc_change_30d': change_30d,
                'volatility': volatility
            }
            
        except Exception as e:
            logger.error(f"Ошибка определения режима рынка: {e}")
            return {
                'regime': 'neutral',
                'suggested_strategy': 'balanced',
                'btc_change_7d': 0.0,
                'btc_change_30d': 0.0,
                'volatility': 0.0
            }

