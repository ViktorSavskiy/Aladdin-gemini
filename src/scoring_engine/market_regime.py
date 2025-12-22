import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import logging

from src.utils.logger import logger

class MarketRegimeDetector:
    """
    Анализирует состояние рынка (Bull/Bear/Neutral) и Sentiment 
    для автоматического выбора оптимальной стратегии скоринга.
    """
    
    @staticmethod
    def analyze_market_condition(market_data: pd.DataFrame, 
                               historical_data: Dict[str, pd.DataFrame],
                               fng_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Определяет режим рынка на основе BTC и Fear & Greed Index.
        
        Args:
            market_data: Текущие рыночные данные.
            historical_data: Словарь с историей цен (обязательно должен быть 'bitcoin').
            fng_data: Данные Fear & Greed (value, classification).
            
        Returns:
            Dict: {'regime': str, 'suggested_strategy': str, 'details': dict}
        """
        # Значения по умолчанию
        result = {
            'regime': 'neutral',
            'suggested_strategy': 'balanced',
            'details': {}
        }
        
        # 1. Получаем данные BTC (Главный индикатор здоровья рынка)
        # Ищем ключ 'bitcoin' или 'btc'
        btc_key = next((k for k in historical_data.keys() if k.lower() in ['bitcoin', 'btc']), None)
        btc_df = historical_data.get(btc_key)
        
        if btc_df is None or btc_df.empty:
            logger.warning("⚠️ Нет истории BTC для анализа рынка. Используем стратегию 'balanced'.")
            return result
            
        # Убедимся, что данные отсортированы по дате
        df = btc_df.sort_values('date').copy()
        prices = df['price'].values
        
        if len(prices) < 30:
            logger.warning("⚠️ Недостаточно истории BTC (<30 дней).")
            return result

        current_price = prices[-1]
        
        # 2. Расчет Технических Индикаторов
        # SMA 50 (Среднесрочный тренд)
        sma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else np.mean(prices)
        
        # Изменение цены за 30 дней (Momentum)
        price_30d_ago = prices[-30]
        change_30d = (current_price - price_30d_ago) / price_30d_ago
        
        # 3. Данные Сентимента (Fear & Greed)
        fng_val = fng_data.get('value', 50) if fng_data else 50
        fng_class = fng_data.get('classification', 'Neutral') if fng_data else 'Neutral'
        
        # 4. ЛОГИКА ОПРЕДЕЛЕНИЯ РЕЖИМА
        
        # Базовые флаги
        is_above_sma = current_price > sma_50
        is_strong_growth = change_30d > 0.10   # Рост > 10% за месяц
        is_crash = change_30d < -0.15          # Падение > 15% за месяц
        is_extreme_fear = fng_val < 20
        is_extreme_greed = fng_val > 80
        
        regime = 'neutral'
        strategy = 'balanced'
        reason = "Рынок без явного тренда"
        
        # --- СЦЕНАРИЙ 1: МЕДВЕЖИЙ РЫНОК (BEAR) ---
        if is_crash or (not is_above_sma and change_30d < 0):
            regime = 'bear'
            strategy = 'bear_defense'
            reason = f"Нисходящий тренд (BTC упал на {change_30d:.1%})"
            
            # Если экстремальный страх на медвежьем рынке - это может быть дно,
            # но безопаснее оставаться в защите.
            if is_extreme_fear:
                reason += " + Extreme Fear (Danger)"

        # --- СЦЕНАРИЙ 2: БЫЧИЙ РЫНОК (BULL) ---
        elif is_above_sma and change_30d > 0:
            regime = 'bull'
            strategy = 'bull_run'
            reason = f"Восходящий тренд (BTC > SMA50, +{change_30d:.1%})"
            
            # Если рост очень слабый, остаемся в balanced
            if change_30d < 0.03: 
                strategy = 'balanced'
                reason = "Слабый рост, используем баланс"
                
            # Если экстремальная жадность - предупреждение
            if is_extreme_greed:
                reason += " [ВНИМАНИЕ: Перегрев рынка!]"

        # --- СЦЕНАРИЙ 3: ВЫКУП СТРАХА (ОТСКОК) ---
        # Если цена выше SMA, но на рынке страх (коррекция в аптренде)
        elif is_above_sma and fng_val < 40:
            regime = 'dip_buy'
            strategy = 'bull_run' # Можно рискнуть
            reason = "Коррекция в аптренде (Buy the Dip opportunities)"

        # 5. Формирование результата
        result = {
            'regime': regime,
            'suggested_strategy': strategy,
            'details': {
                'btc_price': current_price,
                'btc_change_30d': change_30d,
                'sma_50': sma_50,
                'above_sma': is_above_sma,
                'fng_value': fng_val,
                'fng_class': fng_class,
                'reason': reason
            }
        }
        
        # Логирование решения
        logger.info(f"🛡️ ANALYZER: BTC ${current_price:,.0f} | 30d: {change_30d:+.1%} | F&G: {fng_val}")
        logger.info(f"   VERDICT: {regime.upper()} -> Strategy: {strategy} ({reason})")
        
        return result