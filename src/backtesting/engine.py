"""
Модуль векторного бэктестинга (Vectorized Backtesting Engine).
Работает с матрицами Pandas, что в 100 раз быстрее циклов.
"""
import pandas as pd
import numpy as np
from typing import Dict, List
import matplotlib.pyplot as plt
import logging

from src.scoring_engine.strategy_loader import StrategyLoader
from config.settings import Config

logger = logging.getLogger(__name__)

class BacktestEngine:
    def __init__(self, price_matrix: pd.DataFrame):
        self.prices = price_matrix
        # Расчет дневных доходностей (Daily Returns)
        self.daily_returns = self.prices.pct_change()
        
    def run_backtest(self, factor_matrices: Dict[str, pd.DataFrame], 
                    strategy_name: str, 
                    rebalance_days: int = 7,
                    top_n: int = 10) -> Dict:
        """
        Запуск симуляции стратегии.
        """
        logger.info(f"⏳ Запуск бэктеста: {strategy_name}...")
        
        # 1. Загрузка весов
        loader = StrategyLoader()
        strategy = loader.get_strategy(strategy_name)
        weights = strategy.get('weights', {})
        
        # 2. Расчет Комбинированного Счета (Weighted Sum)
        # Создаем пустую матрицу нулей размером как цены
        combined_score = pd.DataFrame(0, index=self.prices.index, columns=self.prices.columns)
        
        for factor_name, weight in weights.items():
            if factor_name in factor_matrices:
                # Score += Factor * Weight
                combined_score += factor_matrices[factor_name] * weight
            elif factor_name not in ['size_large', 'category_advantage']: # Игнорируем статические, если их нет в матрице
                # Для полноценного бэктеста нужны истории MCAP и TVL, 
                # пока используем только ценовые факторы
                pass

        # 3. Генерация Сигналов (Positions)
        # Берем только дни ребалансировки
        # shift(1) - потому что решение принимаем сегодня, а доход получаем завтра
        positions = pd.DataFrame(0.0, index=self.prices.index, columns=self.prices.columns, dtype=float)
        
        # Ребалансировка
        for i in range(0, len(combined_score), rebalance_days):
            date = combined_score.index[i]
            # Топ N монет на эту дату
            day_scores = combined_score.loc[date]
            # Берем топ-N, у которых есть цена (не NaN)
            valid_coins = day_scores[day_scores.notna()].nlargest(top_n).index
            
            # Равновесное распределение (1/N)
            positions.loc[date, valid_coins] = 1.0 / top_n
            
        # Растягиваем позиции вперед до следующей ребалансировки (ffill)
        positions = positions.replace(0, np.nan).ffill(limit=rebalance_days-1).fillna(0)
        
        # 4. Расчет доходности портфеля
        # Strategy Return = Position * Asset Return (shifted by 1 day)
        # Мы покупаем по Close сегодня, держим, получаем доходность Close(T) - Close(T-1)
        
        # Сдвигаем позиции на 1 день вперед (чтобы не заглядывать в будущее)
        lagged_positions = positions.shift(1)
        
        # Доходность стратегии (сумма по всем монетам)
        strategy_daily_ret = (lagged_positions * self.daily_returns).sum(axis=1)
        
        # Учет комиссий (упрощенно)
        # Вычитаем fee каждый раз, когда меняется позиция (turnover)
        turnover = positions.diff().abs().sum(axis=1)
        fees = turnover * Config.BACKTEST_CONFIG.get('fee_rate', 0.001)
        
        net_strategy_ret = strategy_daily_ret - fees
        
        # 5. Сравнение с Бенчмарком (BTC Buy & Hold)
        btc_col = 'bitcoin' if 'bitcoin' in self.daily_returns.columns else self.daily_returns.columns[0]
        benchmark_ret = self.daily_returns[btc_col]
        
        return self._calculate_stats(net_strategy_ret, benchmark_ret)

    def _calculate_stats(self, strategy_ret, benchmark_ret):
        """Расчет статистики (Sharpe, Drawdown, ROI)"""
        # Кумулятивная доходность (Equity Curve)
        equity = (1 + strategy_ret).cumprod()
        bench_equity = (1 + benchmark_ret).cumprod()
        
        # Total Return
        total_ret = equity.iloc[-1] - 1
        
        # CAGR (Годовая)
        days = len(strategy_ret)
        years = days / 365
        cagr = (equity.iloc[-1])**(1/years) - 1 if years > 0 else 0
        
        # Volatility (Annual)
        vol = strategy_ret.std() * np.sqrt(365)
        
        # Sharpe
        sharpe = (strategy_ret.mean() / strategy_ret.std()) * np.sqrt(365) if vol > 0 else 0
        
        # Max Drawdown
        rolling_max = equity.cummax()
        drawdown = (equity - rolling_max) / rolling_max
        max_dd = drawdown.min()
        
        return {
            'total_return': total_ret,
            'cagr': cagr,
            'volatility': vol,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_dd,
            'equity_curve': equity,
            'benchmark_curve': bench_equity
        }