import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Обновляем импорт под класс Config
from config.settings import Config
from src.utils.logger import logger

class DataProcessor:
    """Класс для обработки данных и расчета финансовых метрик"""
    
    @staticmethod
    def _calculate_log_returns(prices: pd.Series) -> pd.Series:
        """Вспомогательный метод: логарифмическая доходность"""
        # np.log(p_t / p_{t-1}) = np.log(p_t) - np.log(p_{t-1})
        return np.log(prices / prices.shift(1)).dropna()

    @staticmethod
    def _align_series(s1: pd.Series, s2: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """Вспомогательный метод: выравнивание двух серий по датам"""
        common_idx = s1.dropna().index.intersection(s2.dropna().index)
        return s1.loc[common_idx], s2.loc[common_idx]

    @staticmethod
    def calculate_returns(prices: pd.Series, periods: List[int] = None) -> Dict[str, float]:
        """Расчет простой доходности за периоды (ROI)"""
        if periods is None:
            periods = Config.METRIC_WINDOWS['returns']
        
        returns = {}
        # Убираем NaN в начале, если они есть
        prices_clean = prices.dropna()
        
        if prices_clean.empty:
            return {f'return_{p}d': np.nan for p in periods}

        current_price = prices_clean.iloc[-1]
        
        for period in periods:
            # Нам нужно (period + 1) точек данных, чтобы сделать сдвиг на period назад
            if len(prices_clean) > period:
                # Берем цену period дней назад
                past_price = prices_clean.iloc[-(period + 1)]
                if past_price > 0:
                    returns[f'return_{period}d'] = (current_price - past_price) / past_price
                else:
                    returns[f'return_{period}d'] = np.nan
            else:
                returns[f'return_{period}d'] = np.nan
        
        return returns
    
    @staticmethod
    def calculate_max_drawdown(prices: pd.Series, window: int = 365) -> float:
        """Расчет максимальной просадки за период"""
        if len(prices) < 2:
            return 0.0
            
        # Берем срез данных
        prices_window = prices.iloc[-window:] if len(prices) > window else prices
        
        # Считаем кумулятивный максимум
        rolling_max = prices_window.cummax()
        drawdown = (prices_window - rolling_max) / rolling_max
        max_drawdown = drawdown.min()
        
        return max_drawdown
    
    @staticmethod
    def calculate_volatility(prices: pd.Series, window: int = None) -> float:
        """Расчет годовой волатильности"""
        if window is None:
            window = Config.METRIC_WINDOWS['volatility']
        
        log_ret = DataProcessor._calculate_log_returns(prices)
        
        if len(log_ret) < window:
            return np.nan
        
        # Берем последние window дней
        subset = log_ret.iloc[-window:]
        
        # std * sqrt(365) для годовой волатильности
        return subset.std() * np.sqrt(365)
    
    @staticmethod
    def calculate_sharpe_ratio(prices: pd.Series, risk_free_rate: float = 0.04, window: int = 90) -> float:
        """Расчет коэффициента Шарпа (годового)"""
        # Считаем дневные изменения
        log_ret = DataProcessor._calculate_log_returns(prices)
        
        if len(log_ret) < window:
            return np.nan
            
        subset = log_ret.iloc[-window:]
        
        mean_ret = subset.mean() * 365  # Годовая доходность
        volatility = subset.std() * np.sqrt(365) # Годовой риск
        
        if volatility == 0:
            return np.nan
            
        return (mean_ret - risk_free_rate) / volatility

    @staticmethod
    def calculate_beta_correlation(asset_prices: pd.Series, btc_prices: pd.Series, 
                                 window: int = 30) -> Tuple[float, float]:
        """Расчет корреляции и беты одной функцией (оптимизация)"""
        # Выравниваем даты
        asset_aligned, btc_aligned = DataProcessor._align_series(asset_prices, btc_prices)
        
        if len(asset_aligned) < window:
            return np.nan, np.nan
            
        # Считаем доходности
        asset_ret = DataProcessor._calculate_log_returns(asset_aligned)
        btc_ret = DataProcessor._calculate_log_returns(btc_aligned)
        
        # Снова выравниваем (т.к. shift создает NaN в начале)
        asset_ret, btc_ret = DataProcessor._align_series(asset_ret, btc_ret)
        
        if len(asset_ret) < window:
            return np.nan, np.nan
            
        # Берем окно
        asset_window = asset_ret.iloc[-window:]
        btc_window = btc_ret.iloc[-window:]
        
        # Корреляция
        corr = asset_window.corr(btc_window)
        
        # Бета = Cov(A, B) / Var(B)
        cov = asset_window.cov(btc_window)
        var = btc_window.var()
        
        beta = cov / var if var != 0 else np.nan
        
        return corr, beta
    
    @staticmethod
    def calculate_all_metrics(historical_data: Dict[str, pd.DataFrame], 
                            market_data: pd.DataFrame) -> pd.DataFrame:
        """
        Главный метод: расчет всех метрик для списка активов.
        """
        logger.info("Начинаем расчет финансовых метрик...")
        
        metrics_list = []
        
        # --- Подготовка данных BTC ---
        btc_series = None
        # Ищем BTC по ID (bitcoin) или символу (BTC) в ключах словаря
        btc_keys = [k for k in historical_data.keys() if k.lower() in ['bitcoin', 'btc']]
        
        if btc_keys:
            btc_df = historical_data[btc_keys[0]].copy()
            # Важно: гарантируем datetime индекс
            btc_df['date'] = pd.to_datetime(btc_df['date'])
            btc_series = btc_df.set_index('date')['price'].sort_index()
            logger.info(f"Данные BTC загружены для сравнения (точек: {len(btc_series)})")
        else:
            logger.warning("Данные BTC не найдены! Корреляция и Бета не будут рассчитаны.")

        # --- Перебор активов ---
        # Создаем маппинг coin_id -> row из market_data для быстрого доступа
        market_info_map = market_data.set_index('coin_id').to_dict('index')

        for coin_id, df in historical_data.items():
            try:
                # Получаем текущие рыночные данные
                info = market_info_map.get(coin_id)
                if not info:
                    continue
                    
                symbol = info.get('symbol', coin_id)
                market_cap = info.get('market_cap', 0)
                
                # Подготовка цен
                df = df.copy()
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                
                prices = df['price']
                if prices.empty:
                    continue

                # --- Расчет метрик ---
                
                # 1. Доходности (7d, 30d)
                returns = DataProcessor.calculate_returns(prices)
                
                # 2. Волатильность и Шарп
                volatility = DataProcessor.calculate_volatility(prices)
                sharpe = DataProcessor.calculate_sharpe_ratio(prices)
                max_dd = DataProcessor.calculate_max_drawdown(prices)
                
                # 3. Корреляция и Бета
                corr, beta = np.nan, np.nan
                if btc_series is not None and coin_id.lower() not in ['bitcoin', 'btc']:
                    corr, beta = DataProcessor.calculate_beta_correlation(
                        prices, btc_series, window=Config.METRIC_WINDOWS['correlation']
                    )
                elif coin_id.lower() in ['bitcoin', 'btc']:
                    corr, beta = 1.0, 1.0

                # 4. Сборка результата
                metric_row = {
                    'coin_id': coin_id,
                    'symbol': symbol,
                    'price': prices.iloc[-1],
                    'market_cap': market_cap,
                    
                    # Метрики
                    'volatility_30d': volatility,
                    'sharpe_90d': sharpe,
                    'max_drawdown_365d': max_dd,
                    'correlation_btc': corr,
                    'beta_btc': beta,
                    
                    # Мета
                    'data_days': len(prices),
                    'last_updated': datetime.now()
                }
                # Добавляем returns (распаковка словаря)
                metric_row.update(returns)
                
                metrics_list.append(metric_row)

            except Exception as e:
                logger.error(f"Ошибка расчета для {coin_id}: {e}")
                continue
        
        # Создаем итоговый DataFrame
        result_df = pd.DataFrame(metrics_list)
        
        if result_df.empty:
            logger.warning("Не удалось рассчитать метрики ни для одного актива.")
            return pd.DataFrame()

        # Округляем для красоты
        float_cols = result_df.select_dtypes(include=['float64']).columns
        result_df[float_cols] = result_df[float_cols].round(4)
        
        logger.info(f"Метрики рассчитаны для {len(result_df)} активов.")
        return result_df