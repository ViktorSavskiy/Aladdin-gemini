import pandas as pd
from typing import List, Dict
import logging

from config.settings import Config
from src.utils.logger import logger

class RebalanceEngine:
    """Генерация торговых сигналов для Bybit"""
    
    def __init__(self):
        self.config = Config.PORTFOLIO_CONFIG
        self.min_trade = self.config.get('min_trade_amount_usd', 10.0)
        self.base_curr = self.config.get('base_currency', 'USDT')

    def generate_rebalance_plan(self, comparison_df: pd.DataFrame) -> List[Dict]:
        """
        Создает список ордеров на основе отклонений.
        """
        if comparison_df.empty: return []
        
        logger.info("🛠 Генерация плана ребалансировки...")
        
        orders = []
        
        # Сортировка: Сначала ПРОДАЖИ (чтобы получить USDT), потом ПОКУПКИ
        # Сортируем так: SELL идут первыми
        df_sorted = comparison_df.sort_values('value_delta', ascending=True)
        
        for _, row in df_sorted.iterrows():
            symbol = row['symbol'].upper()
            action = row['action']
            usd_amount = abs(row['value_delta'])
            
            # Не торгуем USDT
            if symbol == self.base_curr: continue
            if action == 'HOLD': continue
            
            # Проверка на минимальный ордер
            if usd_amount < self.min_trade:
                # Исключение: Если нужно полностью продать актив (Target=0), продаем даже мелочь
                if action == 'SELL' and row['target_weight'] == 0:
                    pass
                else:
                    continue

            # Расчет количества
            price = row.get('current_price', 0)
            if price <= 0: continue
            
            amount_coin = usd_amount / price
            
            # Формируем структуру ордера
            order = {
                'exchange': 'bybit',
                'symbol': f"{symbol}/{self.base_curr}", # ETH/USDT
                'side': action.lower(),                 # 'buy' или 'sell'
                'type': 'market',                       # Рыночный
                'amount_coin': amount_coin,
                'amount_usd': usd_amount,
                'reason': f"Target: {row['target_weight']:.1%} | Curr: {row['current_weight']:.1%}"
            }
            
            orders.append(order)
            
        logger.info(f"Сформировано {len(orders)} ордеров.")
        return orders