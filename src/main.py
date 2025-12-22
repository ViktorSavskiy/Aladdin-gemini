import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
# ... импорты ...
from src.portfolio_analyzer.portfolio_loader import PortfolioLoader
from src.portfolio_analyzer.portfolio_metrics import PortfolioMetrics
from src.portfolio_analyzer.comparator import PortfolioComparator
from src.portfolio_analyzer.rebalance_engine import RebalanceEngine
from src.portfolio_analyzer.report_generator import PortfolioReportGenerator
# --- Настройка путей для импорта ---
current_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(current_dir))

# --- Импорты конфигурации ---
from config.settings import Config

# --- Импорты Модуля 1 (Data Pipeline) ---
from src.data_pipeline.data_fetcher import DataFetcher
from src.data_pipeline.specific_fetcher import CategoryFetcher
from src.data_pipeline.sentiment_fetcher import SentimentFetcher # НОВЫЙ ИМПОРТ
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
    Управляет потоками данных, AI-оценкой и отчетностью.
    """
    
    def __init__(self):
        # 1. Инициализация компонентов данных
        self.fetcher = DataFetcher()
        self.specific_fetcher = CategoryFetcher()  # DefiLlama
        self.sentiment_fetcher = SentimentFetcher() # News AI & Fear/Greed
        self.filter = DataFilter()
        self.processor = DataProcessor()
        self.db_handler = DatabaseHandler()
        
        # 2. Инициализация компонентов оценки
        self.strategy_loader = StrategyLoader()
        self.score_calculator = ScoreCalculator(self.strategy_loader)
        self.portfolio_loader = PortfolioLoader()
        self.comparator = PortfolioComparator()
        self.rebalancer = RebalanceEngine()
    def _ensure_btc_history(self, historical_data: dict, coin_ids: list) -> dict:
        """Гарантирует наличие истории BTC (нужно для корреляции)"""
        if 'bitcoin' not in historical_data:
            logger.info("BTC отсутствует в выборке. Загружаем историю BTC отдельно...")
            btc_data = self.fetcher.fetch_historical_data('bitcoin', days=Config.HISTORICAL_DAYS)
            if not btc_data.empty:
                historical_data['bitcoin'] = btc_data
        return historical_data

    def run_full_pipeline(self, use_existing_data: bool = False, run_backtest: bool = False):
        """
        Запуск полного цикла.
        Args:
            use_existing_data: True = Быстро (из БД), False = Обновление данных.
            run_backtest: True = Запуск симуляции на истории.
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ЗАПУСК CRYPTO ALADDIN: AI EDITION")
            logger.info(f"⚙️  Режим: {'DEV (Из базы)' if use_existing_data else 'PROD (Обновление)'}")
            logger.info("=" * 60)
            
            # Переменные данных
            metrics_df = pd.DataFrame()
            historical_data = {}
            onchain_data = pd.DataFrame()
            category_df = pd.DataFrame()
            market_data = pd.DataFrame()
            filtered_data = pd.DataFrame()
            
            # 0. Сбор Сентимента (Быстро)
            fng_data = self.sentiment_fetcher.fetch_fear_and_greed()
            logger.info(f"😱 Индекс Страха: {fng_data.get('value', 'N/A')} ({fng_data.get('classification', 'N/A')})")

            # ==========================================
            # БЛОК 1: СБОР ДАННЫХ (ETL)
            # ==========================================
            
            if use_existing_data:
                logger.info("💾 [1/7] Загрузка данных из локальной базы...")
                try:
                    metrics_df = self.db_handler.get_latest_metrics()
                    if metrics_df.empty:
                        logger.error("❌ Метрики в базе не найдены. Запустите с use_existing_data=False")
                        return
                    
                    try:
                        category_df = pd.read_sql("SELECT * FROM asset_categories WHERE date = (SELECT MAX(date) FROM asset_categories)", self.db_handler.engine)
                        onchain_data = self.db_handler.get_latest_onchain_data()
                        market_data = self.db_handler.get_latest_market_data(days=1)
                        filtered_data = self.db_handler.get_filtered_assets()
                    except Exception as e:
                        logger.warning(f"Часть данных не загружена (некритично): {e}")

                except Exception as e:
                    logger.error(f"Ошибка чтения базы: {e}")
                    return

            else:
                logger.info("📡 [1/7] Сбор свежих данных с API...")
                self.db_handler._init_db()
                
                # 1.1 Рынок
                market_data = self.fetcher.fetch_coingecko_market_data()
                if market_data.empty: return
                self.db_handler.save_market_data(market_data)
                
                # 1.2 Фильтр
                filtered_data = self.filter.apply_all_filters(market_data, exclude_stables=True)
                self.db_handler.save_filtered_assets(filtered_data)
                
                # 1.3 История
                coin_ids = filtered_data['coin_id'].tolist()
                historical_data = self.fetcher.fetch_all_historical_data(coin_ids, days=Config.HISTORICAL_DAYS)
                historical_data = self._ensure_btc_history(historical_data, coin_ids)
                self.db_handler.save_historical_data(historical_data)
                
                # 1.4 On-Chain
                logger.info("⛓️ Сбор On-Chain метрик...")
                coin_list = filtered_data[['coin_id', 'symbol', 'market_cap']].to_dict('records')
                onchain_data = self.fetcher.fetch_onchain_data(coin_list)
                if not onchain_data.empty:
                    self.db_handler.save_onchain_data(onchain_data)
                
                # 1.5 DefiLlama
                logger.info("🦙 Сбор DeFi/L2 метрик...")
                category_df = self.specific_fetcher.fetch_specific_metrics(coin_list)
                if not category_df.empty:
                    self.db_handler.save_category_data(category_df)
                
                # 1.6 Расчет метрик
                logger.info("🧮 Расчет индикаторов...")
                metrics_df = self.processor.calculate_all_metrics(historical_data, market_data)
                self.db_handler.save_metrics(metrics_df)
                
                self.db_handler.cleanup_old_data()

            # ==========================================
            # БЛОК 2: АНАЛИЗ И СКОРИНГ
            # ==========================================
            logger.info("-" * 60)
            logger.info("🧠 [2/7] ЗАПУСК SCORING ENGINE")
            
            # 2.1 Подготовка единого DataFrame
            full_data = metrics_df.copy()
            
            if not onchain_data.empty:
                cols = ['coin_id', 'developer_score', 'messari_active_addresses']
                exist = [c for c in cols if c in onchain_data.columns]
                full_data = pd.merge(full_data, onchain_data[exist], on='coin_id', how='left')
            
            if not category_df.empty:
                cat_cols = ['coin_id', 'category', 'tvl', 'tvl_ratio']
                exist = [c for c in cat_cols if c in category_df.columns]
                full_data = pd.merge(full_data, category_df[exist], on='coin_id', how='left')

            # 2.2 Расчет Факторов
            factors_df = FactorCalculator.calculate_all_factors(full_data, category_df)
            
            # 2.3 Режим Рынка
            # Если истории нет (режим базы), грузим BTC
            if not historical_data and use_existing_data:
                try:
                    btc_hist = self.db_handler.get_historical_data('bitcoin', days=90)
                    if not btc_hist.empty: historical_data = {'bitcoin': btc_hist}
                except: pass

            market_regime = MarketRegimeDetector.analyze_market_condition(
                market_data, historical_data, fng_data
            )
            active_strategy_name = market_regime['suggested_strategy']
            logger.info(f"🛡 РЕЖИМ: {market_regime['regime'].upper()} -> Стратегия: {active_strategy_name}")

            # 2.4 Загрузка стратегий
            strat_path = Config.BASE_DIR / "config" / "strategies.yaml"
            if strat_path.exists():
                self.strategy_loader.load_custom_strategies(str(strat_path))
            
            # 2.5 Расчет Баллов
            scores = self.score_calculator.calculate_dual_scores(
                factors_df,
                long_strat=active_strategy_name,
                short_strat='short_speculative'
            )
            
            # 2.6 Ранжирование
            final_ranking = AssetRanker.create_combined_ranking(scores['long'], scores['short'])
            self.db_handler.save_scores(final_ranking)
            
            # 2.7 AI Анализ Новостей (Контекст)
            logger.info("📰 AI Анализ новостей для ТОП-активов...")
            top_symbols = final_ranking.head(5)['symbol'].tolist() if not final_ranking.empty else []
            news_items = self.sentiment_fetcher.fetch_news_for_coins(top_symbols)

            # 2.8 Генерация Отчета
            logger.info(AssetRanker.get_final_report_data(final_ranking))
            self.save_full_report(final_ranking, full_data, active_strategy_name, fng_data, news_items)
            # ==========================================
            # БЛОК 4: АНАЛИЗ ПОРТФЕЛЯ (BYBIT)
            # ==========================================
            logger.info("-" * 60)
            logger.info("💼 [4/7] ЗАПУСК PORTFOLIO ANALYZER (Bybit)")
            
            # 1. Загрузка реального портфеля
            # Нам нужны текущие цены из market_data для оценки стоимости
            current_portfolio = self.portfolio_loader.load_portfolio(market_data)
            
            if not current_portfolio.empty:
                # 2. Метрики здоровья портфеля
                # Передаем таблицу с рейтингами (final_ranking), чтобы оценить качество активов
                port_stats = PortfolioMetrics.calculate_portfolio_stats(current_portfolio, final_ranking)
                
                logger.info(f"Стоимость портфеля: ${port_stats.get('total_value_usd', 0):.2f}")
                logger.info(f"Aladdin Health Score: {port_stats.get('aladdin_health_score', 0):.1f}/100")
                
                # 3. Сравнение с Идеальным Портфелем (из Scoring Engine)
                # final_ranking - это наш идеальный список покупок
                comparison = self.comparator.compare_portfolios(current_portfolio, final_ranking)
                
                # 4. Генерация плана действий
                rebalance_orders = self.rebalancer.generate_rebalance_plan(comparison)
                
                # 5. Отчет
                report_path = PortfolioReportGenerator.generate_rebalance_report(
                    comparison, rebalance_orders, port_stats
                )
                logger.info(f"📄 План действий сохранен: {report_path}")
                
            else:
                logger.warning("Портфель пуст или ошибка соединения с Bybit.")
            # ==========================================
            # БЛОК 3: БЭКТЕСТИНГ
            # ==========================================
            if run_backtest:
                logger.info("-" * 60)
                logger.info("🕹️ [3/7] ЗАПУСК БЭКТЕСТА")
                
                # Подгрузка истории при необходимости
                if not historical_data:
                    logger.info("Подгрузка истории из базы...")
                    top_coins = final_ranking['coin_id'].tolist() if not final_ranking.empty else []
                    for cid in top_coins[:30]: 
                         df = self.db_handler.get_historical_data(cid, days=730)
                         if not df.empty: historical_data[cid] = df
                
                if historical_data:
                    price_matrix = FactorCalculator.prepare_price_matrix(historical_data)
                    if not price_matrix.empty:
                        logger.info("Расчет исторических факторов...")
                        rolling_factors = FactorCalculator.calculate_rolling_factors(price_matrix)
                        
                        engine = BacktestEngine(price_matrix)
                        strategies = ['balanced', 'bull_run', 'bear_defense', 'defi_value']
                        if active_strategy_name not in strategies: strategies.append(active_strategy_name)
                            
                        logger.info("\n📊 ИСТОРИЧЕСКАЯ СИМУЛЯЦИЯ (2 года):")
                        logger.info(f"{'Strategy':<15} {'Return':<10} {'Sharpe':<8} {'MaxDD':<8}")
                        logger.info("-" * 45)
                        
                        for strat in strategies:
                            res = engine.run_backtest(rolling_factors, strat)
                            logger.info(
                                f"{strat:<15} {res['total_return']:<10.1%} {res['sharpe_ratio']:<8.2f} {res['max_drawdown']:<8.1%}"
                            )
                        logger.info("-" * 45)
                    else:
                        logger.warning("Нет цен для бэктеста.")
                else:
                    logger.warning("История пуста.")

            logger.info("=" * 60)
            logger.info("✅ АНАЛИЗ ЗАВЕРШЕН")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)

    def save_full_report(self, ranking_df, full_data, strategy_name, fng_data, news):
        """Сохранение подробного AI-отчета"""
        try:
            report_path = Config.DATA_DIR / "reports" / "final_report.txt"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"CRYPTO ALADDIN AI REPORT | {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Active Strategy: {strategy_name}\n")
                f.write(f"Sentiment Index: {fng_data.get('value', 0)} ({fng_data.get('classification', 'N/A')})\n")
                f.write("="*80 + "\n\n")
                
                if 'category' in full_data.columns:
                    f.write("SECTOR DISTRIBUTION:\n")
                    counts = full_data['category'].value_counts()
                    for cat, count in counts.items():
                        f.write(f"- {cat}: {count}\n")
                    f.write("\n")

                f.write("🏆 TOP BUY RECOMMENDATIONS (Long Score):\n")
                f.write("-" * 80 + "\n")
                f.write(f"{'Symbol':<8} {'Score':<8} {'Net':<8} {'Signal':<12} {'Driver':<15}\n")
                
                top_buy = ranking_df.head(15)
                for _, row in top_buy.iterrows():
                    driver = str(row['primary_driver'])[:15]
                    f.write(
                        f"{row['symbol']:<8} {row['score_long']:<8.1f} {row['net_score']:<8.1f} "
                        f"{row['signal']:<12} {driver:<15}\n"
                    )
                
                f.write("\n🐻 TOP SELL/HEDGE CANDIDATES:\n")
                f.write("-" * 80 + "\n")
                top_sell = ranking_df.sort_values('score_short', ascending=False).head(10)
                for _, row in top_sell.iterrows():
                    driver = str(row['primary_driver'])[:15]
                    f.write(
                        f"{row['symbol']:<8} {row['score_short']:<8.1f} {row['net_score']:<8.1f} "
                        f"{row['signal']:<12} {driver:<15}\n"
                    )

                if news:
                    f.write("\n📰 AI NEWS SENTIMENT ANALYSIS:\n")
                    f.write("-" * 80 + "\n")
                    f.write(f"{'Label':<6} {'Score':<6} {'Coins':<10} {'Title'}\n")
                    f.write("-" * 80 + "\n")
                    
                    for item in news:
                        title = (item['title'][:60] + '..') if len(item['title']) > 60 else item['title']
                        coins = ",".join(item.get('currencies', []))[:10]
                        label = item.get('sentiment_label', 'NEUT')
                        score = item.get('sentiment_score', 0.0)
                        
                        f.write(f"{label:<6} {score:<+6.2f} {coins:<10} {title}\n")

            logger.info(f"📄 Полный отчет сохранен: {report_path}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения отчета: {e}")

def main():
    try:
        Config.setup_directories()
        pipeline = CryptoAladdinPipeline()
        
        # use_existing_data=True (БЫСТРО, из базы)
        # use_existing_data=False (ОБНОВЛЕНИЕ, с интернета)
        pipeline.run_full_pipeline(
            use_existing_data=True, 
            run_backtest=True       
        )
        
    except KeyboardInterrupt:
        logger.info("Программа остановлена")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()