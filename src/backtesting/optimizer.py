import pandas as pd
import numpy as np
import itertools
from typing import Dict, List
import sys
from pathlib import Path

# Настройка путей
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.data_pipeline.database_handler import DatabaseHandler
from src.scoring_engine.factor_calculator import FactorCalculator
from src.backtesting.engine import BacktestEngine
from src.utils.logger import logger

class StrategyOptimizer:
    """
    Перебирает комбинации весов (Grid Search), чтобы найти лучшую стратегию.
    """
    
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.prices = None
        self.factors = None
        
    def load_data(self):
        """Загрузка данных из базы для оптимизации"""
        logger.info("Загрузка данных для оптимизации...")
        # 1. Загружаем историю (пакетно или всех filtered assets)
        assets = self.db_handler.get_filtered_assets()
        if assets.empty:
            logger.error("Нет активов в базе. Запустите main.py хотя бы раз.")
            return False
            
        coin_ids = assets['coin_id'].tolist()
        # Для скорости берем топ-30 по капитализации
        top_coins = assets.nlargest(30, 'market_cap')['coin_id'].tolist()
        
        # Получаем историю (предполагаем, что она есть в базе)
        # Если метода get_batch нет, используем цикл (для оптимизатора это ок)
        hist_data = {}
        logger.info("Чтение истории цен...")
        for cid in top_coins:
            df = self.db_handler.get_historical_data(cid, days=730)
            if not df.empty:
                hist_data[cid] = df
                
        if not hist_data:
            logger.error("История пуста.")
            return False

        # 2. Готовим матрицы
        self.prices = FactorCalculator.prepare_price_matrix(hist_data)
        logger.info("Расчет факторов...")
        self.factors = FactorCalculator.calculate_rolling_factors(self.prices)
        
        return True

    def run_optimization(self):
        if self.prices is None:
            if not self.load_data(): return

        engine = BacktestEngine(self.prices)
        
        # ОПРЕДЕЛЯЕМ ПАРАМЕТРЫ ДЛЯ ПЕРЕБОРА
        # Мы ищем баланс между тремя факторами: Импульс, Риск, Качество
        # Сумма весов должна быть примерно 1.0
        
        # Генерируем сетку весов (шаг 0.2)
        # Например: Momentum от 0.0 до 0.8
        r = np.arange(0, 1.1, 0.2)
        
        results = []
        
        logger.info("🚀 Запуск перебора комбинаций...")
        
        # Перебираем 3 основных фактора
        for w_mom in r:
            for w_vol in r:
                for w_qual in r:
                    # Проверяем, чтобы сумма была близка к 1.0 (0.8-1.2 ок)
                    total = w_mom + w_vol + w_qual
                    if not (0.9 <= total <= 1.1):
                        continue
                        
                    # Создаем "виртуальную" стратегию
                    # Важно использовать правильные ключи факторов из FactorCalculator!
                    temp_weights = {
                        'momentum_30d': w_mom,
                        'low_volatility': w_vol,
                        'quality_sharpe': w_qual
                    }
                    
                    # Запускаем упрощенный бэктест
                    # Нам нужно слегка модифицировать engine.run_backtest, 
                    # чтобы он принимал словарь весов напрямую, а не имя стратегии.
                    # Но пока используем хак: подменим loader внутри engine (сложно)
                    # ПРОЩЕ: Вызовем расчет Score вручную здесь.
                    
                    stats = self._quick_backtest(engine, temp_weights)
                    
                    results.append({
                        'w_mom': w_mom,
                        'w_vol': w_vol,
                        'w_qual': w_qual,
                        'Sharpe': stats['sharpe_ratio'],
                        'Return': stats['total_return'],
                        'MaxDD': stats['max_drawdown']
                    })
        
        # Анализ результатов
        results_df = pd.DataFrame(results)
        
        # Топ-5 по Шарпу
        print("\n🏆 ТОП-5 КОМБИНАЦИЙ (по Шарпу):")
        print(results_df.sort_values('Sharpe', ascending=False).head(5))
        
        # Топ-5 по Доходности
        print("\n🤑 ТОП-5 КОМБИНАЦИЙ (по Доходности):")
        print(results_df.sort_values('Return', ascending=False).head(5))

    def _quick_backtest(self, engine, weights):
        """Быстрый расчет без создания классов стратегий"""
        # 1. Считаем Combined Score
        combined_score = pd.DataFrame(0, index=engine.prices.index, columns=engine.prices.columns)
        for factor, w in weights.items():
            if factor in self.factors:
                combined_score += self.factors[factor] * w
                
        # 2. Симуляция (копия логики из engine.py)
        # Ребаланс каждые 7 дней, топ-5 монет
        positions = pd.DataFrame(0, index=engine.prices.index, columns=engine.prices.columns)
        
        # Векторный способ ребалансировки (быстрее цикла)
        # Берем данные только по пятницам (или каждый 7-й день)
        rebalance_idx = combined_score.index[::7]
        
        # Для каждой даты ребалансировки находим топ-5
        # (в цикле, так как rank по строкам сложен в векторе с топ-N)
        for date in rebalance_idx:
            day_scores = combined_score.loc[date]
            # Топ 5
            top_coins = day_scores.nlargest(5).index
            positions.loc[date, top_coins] = 0.2 # 1/5 = 20%
            
        positions = positions.ffill().fillna(0)
        
        lagged_pos = positions.shift(1)
        strat_ret = (lagged_pos * engine.daily_returns).sum(axis=1)
        
        # Статистика
        ann_ret = strat_ret.mean() * 365
        ann_vol = strat_ret.std() * np.sqrt(365)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        
        # Equity curve для просадки
        equity = (1 + strat_ret).cumprod()
        dd = (equity - equity.cummax()) / equity.cummax()
        max_dd = dd.min()
        
        return {'sharpe_ratio': sharpe, 'total_return': equity.iloc[-1] - 1, 'max_drawdown': max_dd}

if __name__ == "__main__":
    opt = StrategyOptimizer()
    opt.run_optimization()