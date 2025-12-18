import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# --- Настройка путей для импорта ---
current_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(current_dir))

# --- Импорты конфигурации ---
from config.settings import Config

# --- Импорты Модуля 1 (Data Pipeline) ---
from src.data_pipeline.data_fetcher import DataFetcher
from src.data_pipeline.specific_fetcher import CategoryFetcher
from src.data_pipeline.filters import DataFilter
from src.data_pipeline.data_processor import DataProcessor
from src.data_pipeline.database_handler import DatabaseHandler

# --- Импорты Модуля 2 (Scoring Engine) ---
from src.scoring_engine.factor_calculator import FactorCalculator
from src.scoring_engine.strategy_loader import StrategyLoader
from src.scoring_engine.score_calculator import ScoreCalculator
from src.scoring_engine.ranking import AssetRanker
from src.scoring_engine.market_regime import MarketRegimeDetector

# --- Импорты Бэктестинга ---
from src.backtesting.engine import BacktestEngine

# --- Утилиты ---
from src.utils.logger import logger

class CryptoAladdinPipeline:
    """
    Главный оркестратор системы Crypto Aladdin.
    Управляет потоками данных, оценкой активов и симуляцией торговли.
    """
    
    def __init__(self):
        # 1. Инициализация компонентов данных
        self.fetcher = DataFetcher()
        self.specific_fetcher = CategoryFetcher()  # DefiLlama & Categories
        self.filter = DataFilter()
        self.processor = DataProcessor()
        self.db_handler = DatabaseHandler()
        
        # 2. Инициализация компонентов оценки
        self.strategy_loader = StrategyLoader()
        self.score_calculator = ScoreCalculator(self.strategy_loader)

    def _ensure_btc_history(self, historical_data: dict, coin_ids: list) -> dict:
        """Гарантирует наличие истории BTC (нужно для корреляции и режима рынка)"""
        if 'bitcoin' not in historical_data:
            logger.info("BTC отсутствует в выборке. Загружаем историю BTC отдельно...")
            # Используем max для глубокой истории или значение из конфига
            btc_data = self.fetcher.fetch_historical_data('bitcoin', days=Config.HISTORICAL_DAYS)
            if not btc_data.empty:
                historical_data['bitcoin'] = btc_data
        return historical_data

    def run_full_pipeline(self, use_existing_data: bool = False, run_backtest: bool = False):
        """
        Запуск полного цикла.
        Args:
            use_existing_data: Если True, берет данные из локальной БД (быстро).
            run_backtest: Если True, запускает симуляцию стратегий на истории.
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ЗАПУСК CRYPTO ALADDIN: PC EDITION")
            logger.info(f"⚙️  Режим: {'DEV (Из базы)' if use_existing_data else 'PROD (Обновление данных)'}")
            logger.info(f"📈 Бэктест: {'Включен' if run_backtest else 'Выключен'}")
            logger.info("=" * 60)
            
            # Переменные для хранения данных
            metrics_df = pd.DataFrame()
            historical_data = {}
            onchain_data = pd.DataFrame()
            category_df = pd.DataFrame()
            market_data = pd.DataFrame()
            filtered_data = pd.DataFrame()

            # ==========================================
            # БЛОК 1: СБОР ДАННЫХ (ETL)
            # ==========================================
            
            if use_existing_data:
                logger.info("💾 [1/7] Загрузка данных из локальной базы...")
                try:
                    metrics_df = self.db_handler.get_latest_metrics()
                    
                    # Загружаем историю для всех монет (нужно для бэктеста)
                    # Внимание: это может быть долго, если монет много
                    filtered_assets = self.db_handler.get_filtered_assets()
                    if not filtered_assets.empty:
                        coin_ids = filtered_assets['coin_id'].tolist()
                        # Здесь предполагаем, что у db_handler есть метод пакетной загрузки
                        # Если нет - загрузится то, что есть, или нужно реализовать get_historical_batch
                        # Для упрощения пока грузим только метрики, а историю подтянем ниже если надо
                        pass 
                    
                    # Пытаемся загрузить остальное
                    try:
                        # Читаем последнюю дату категорий
                        category_df = pd.read_sql("SELECT * FROM asset_categories WHERE date = (SELECT MAX(date) FROM asset_categories)", self.db_handler.engine)
                        onchain_data = self.db_handler.get_latest_onchain_data()
                        market_data = self.db_handler.get_latest_market_data(days=1)
                    except Exception as e:
                        logger.warning(f"Часть данных не загружена из базы: {e}")

                    if metrics_df.empty:
                        logger.error("❌ Метрики в базе не найдены. Запустите с use_existing_data=False")
                        return

                except Exception as e:
                    logger.error(f"Ошибка чтения базы: {e}")
                    return

            else:
                logger.info("📡 [1/7] Сбор свежих данных с API...")
                self.db_handler._init_db()
                
                # 1.1 Рыночные данные (CoinGecko)
                market_data = self.fetcher.fetch_coingecko_market_data()
                if market_data.empty: 
                    logger.error("Не удалось получить рыночные данные")
                    return
                self.db_handler.save_market_data(market_data)
                
                # 1.2 Фильтрация
                filtered_data = self.filter.apply_all_filters(market_data, exclude_stables=True)
                self.db_handler.save_filtered_assets(filtered_data)
                logger.info(f"Отобрано активов: {len(filtered_data)}")
                
                # 1.3 История цен (Deep History)
                coin_ids = filtered_data['coin_id'].tolist()
                historical_data = self.fetcher.fetch_all_historical_data(
                    coin_ids, 
                    days=Config.HISTORICAL_DAYS # Теперь 730 дней (2 года)
                )
                historical_data = self._ensure_btc_history(historical_data, coin_ids)
                self.db_handler.save_historical_data(historical_data)
                
                # 1.4 On-Chain данные (GitHub / Messari)
                logger.info("⛓️ Сбор On-Chain метрик...")
                coin_list = filtered_data[['coin_id', 'symbol', 'market_cap']].to_dict('records')
                onchain_data = self.fetcher.fetch_onchain_data(coin_list)
                if not onchain_data.empty:
                    self.db_handler.save_onchain_data(onchain_data)
                
                # 1.5 Специфичные метрики (DefiLlama / Categories)
                logger.info("🦙 Сбор DeFi/L2 метрик (DefiLlama)...")
                category_df = self.specific_fetcher.fetch_specific_metrics(coin_list)
                if not category_df.empty:
                    self.db_handler.save_category_data(category_df)
                
                # 1.6 Расчет технических метрик
                logger.info("🧮 Расчет технических индикаторов...")
                metrics_df = self.processor.calculate_all_metrics(historical_data, market_data)
                self.db_handler.save_metrics(metrics_df)
                
                # 1.7 Очистка
                self.db_handler.cleanup_old_data()

            # ==========================================
            # БЛОК 2: АНАЛИЗ И СКОРИНГ
            # ==========================================
            logger.info("-" * 60)
            logger.info("🧠 [2/7] ЗАПУСК SCORING ENGINE")
            
            # 2.1 Подготовка единого DataFrame
            full_data = metrics_df.copy()
            
            # Мержим On-Chain
            if not onchain_data.empty:
                cols = ['coin_id', 'developer_score', 'messari_active_addresses']
                exist = [c for c in cols if c in onchain_data.columns]
                full_data = pd.merge(full_data, onchain_data[exist], on='coin_id', how='left')
            
            # Мержим Категории (TVL)
            if not category_df.empty:
                cat_cols = ['coin_id', 'category', 'tvl', 'tvl_ratio']
                exist = [c for c in cat_cols if c in category_df.columns]
                full_data = pd.merge(full_data, category_df[exist], on='coin_id', how='left')

            # 2.2 Расчет Факторов (Z-Scores)
            factors_df = FactorCalculator.calculate_all_factors(full_data, category_df)
            
            # 2.3 Определение Режима Рынка
            # Если historical_data пуст (режим из базы), попробуем загрузить BTC отдельно
            if not historical_data and use_existing_data:
                try:
                    btc_hist = self.db_handler.get_historical_data('bitcoin', days=90)
                    historical_data = {'bitcoin': btc_hist}
                except: pass

            market_regime = MarketRegimeDetector.analyze_market_condition(
                market_data, historical_data
            )
            
            active_strategy_name = market_regime['suggested_strategy']
            logger.info(f"🛡 РЕЖИМ РЫНКА: {market_regime['regime'].upper()}")
            logger.info(f"🎯 ВЫБРАНА СТРАТЕГИЯ: {active_strategy_name}")

            # 2.4 Загрузка конфигурации стратегий
            strat_path = Config.BASE_DIR / "config" / "strategies.yaml"
            if strat_path.exists():
                self.strategy_loader.load_custom_strategies(str(strat_path))
            
            # 2.5 Расчет Баллов (Scoring)
            scores = self.score_calculator.calculate_dual_scores(
                factors_df,
                long_strat=active_strategy_name,
                short_strat='short_speculative'
            )
            
            # 2.6 Ранжирование (Ranking)
            final_ranking = AssetRanker.create_combined_ranking(scores['long'], scores['short'])
            
            # 2.7 Сохранение
            self.db_handler.save_scores(final_ranking)
            
            # 2.8 Отчет в лог
            logger.info(AssetRanker.get_final_report_data(final_ranking))
            self.save_full_report(final_ranking, full_data, active_strategy_name)

            # ==========================================
            # БЛОК 3: БЭКТЕСТИНГ (Vectorized)
            # ==========================================
            if run_backtest:
                logger.info("-" * 60)
                logger.info("🕹️ [3/7] ЗАПУСК БЭКТЕСТА (Backtesting Engine)")
                
                # Если истории нет в памяти (режим use_existing_data), нужно её загрузить
                if not historical_data:
                    logger.info("Загрузка истории из базы для бэктеста (это может