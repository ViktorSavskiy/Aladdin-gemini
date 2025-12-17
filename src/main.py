import sys
import pandas as pd
from pathlib import Path

# Добавляем корневую директорию в PYTHONPATH
current_dir = Path(__file__).resolve().parent
root_dir = current_dir.parent if current_dir.name == 'src' else current_dir
sys.path.append(str(root_dir))

from config.settings import Config
from src.data_pipeline.data_fetcher import DataFetcher
from src.data_pipeline.filters import DataFilter
from src.data_pipeline.data_processor import DataProcessor
from src.data_pipeline.database_handler import DatabaseHandler
from src.utils.logger import logger

class CryptoAladdinPipeline:
    """
    Главный оркестратор: Сбор -> Фильтрация -> История + OnChain -> Метрики -> База -> Отчет
    """
    
    def __init__(self):
        self.fetcher = DataFetcher()
        self.filter = DataFilter()
        self.processor = DataProcessor()
        self.db_handler = DatabaseHandler()
        
    def _ensure_btc_history(self, historical_data: dict, coin_ids: list) -> dict:
        """Гарантирует наличие истории BTC для расчета корреляции."""
        btc_id = 'bitcoin'
        if btc_id not in historical_data:
            logger.info("BTC отсутствует в списке. Загружаем историю BTC для корреляции...")
            btc_data = self.fetcher.fetch_historical_data(btc_id, days=Config.HISTORICAL_DAYS)
            if not btc_data.empty:
                historical_data[btc_id] = btc_data
        return historical_data

    def run_full_pipeline(self):
        """Запуск полного цикла обновления данных"""
        try:
            logger.info("=" * 60)
            logger.info("🚀 ЗАПУСК CRYPTO ALADDIN PIPELINE")
            logger.info("=" * 60)
            
            # --- Шаг 1: Рыночные данные ---
            logger.info("[1/7] Получение текущих рыночных данных...")
            market_data = self.fetcher.fetch_coingecko_market_data()
            
            if market_data.empty:
                logger.error("❌ Остановка: Не удалось получить рыночные данные.")
                return
            
            self.db_handler.save_market_data(market_data)
            
            # --- Шаг 2: Фильтрация ---
            logger.info("[2/7] Фильтрация и категоризация активов...")
            filtered_data = self.filter.apply_all_filters(market_data, exclude_stables=True)
            
            if filtered_data.empty:
                logger.error("❌ Остановка: Нет активов, прошедших фильтры.")
                return
                
            self.db_handler.save_filtered_assets(filtered_data)
            
            # --- Шаг 3: История цен ---
            coin_ids = filtered_data['coin_id'].tolist()
            logger.info(f"[3/7] Сбор истории цен для {len(coin_ids)} активов...")
            
            historical_data = self.fetcher.fetch_all_historical_data(
                coin_ids, 
                days=Config.HISTORICAL_DAYS
            )
            
            historical_data = self._ensure_btc_history(historical_data, coin_ids)
            self.db_handler.save_historical_data(historical_data)
            
            # --- Шаг 4: Сбор On-Chain данных (НОВОЕ) ---
            logger.info(f"[4/7] Сбор On-Chain метрик (Fundamental)...")
            
            # ВАЖНО: Добавил 'market_cap' для расчета NVT Ratio
            coin_list_for_onchain = filtered_data[['coin_id', 'symbol', 'name', 'market_cap']].to_dict('records')
            
            onchain_data = self.fetcher.fetch_onchain_data(coin_list_for_onchain)

            if not onchain_data.empty:
                self.db_handler.save_onchain_data(onchain_data)
                logger.info(f"On-Chain данные сохранены для {len(onchain_data)} монет")
            else:
                logger.warning("Не удалось собрать on-chain данные (возможно, лимиты API)")
            
            # --- Шаг 5: Расчет финансовых метрик ---
            logger.info("[5/7] Расчет финансовых метрик (Volatility, Sharpe, Beta)...")
            metrics_df = self.processor.calculate_all_metrics(
                historical_data, 
                market_data
            )
            
            self.db_handler.save_metrics(metrics_df)
            
            # --- Шаг 6: Очистка старого ---
            logger.info("[6/7] Очистка устаревших данных из БД...")
            self.db_handler.cleanup_old_data(days_to_keep=365)
            
            # --- Шаг 7: Отчет ---
            logger.info("[7/7] Генерация отчета...")
            self.generate_report(metrics_df, filtered_data, onchain_data)
            
            logger.info("=" * 60)
            logger.info("✅ ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН")
            logger.info("=" * 60)
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Процесс остановлен пользователем.")
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в пайплайне: {e}", exc_info=True)
    
    def generate_report(self, metrics_df: pd.DataFrame, filtered_data: pd.DataFrame, onchain_df: pd.DataFrame = None):
        """Генерация текстового отчета"""
        try:
            timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            
            report_lines = [
                "\n" + "=" * 60,
                f"ОТЧЕТ CRYPTO ALADDIN | {timestamp}",
                "=" * 60,
                f"Всего проанализировано: {len(filtered_data)} активов",
                f"Финансовые метрики:     {len(metrics_df)} активов",
                f"On-Chain данные:        {len(onchain_df) if onchain_df is not None else 0} активов",
                "\n🏆 ТОП-10 АКТИВОВ ПО КАПИТАЛИЗАЦИИ:",
                "-" * 65,
                f"{'Symbol':<10} {'Name':<18} {'Cap ($B)':<10} {'Price ($)':<10}"
            ]
            
            # Топ-10 Cap
            top_10 = filtered_data.sort_values('market_cap', ascending=False).head(10)
            for _, row in top_10.iterrows():
                cap_b = row.get('market_cap', 0) / 1e9
                price = row.get('price', 0)
                report_lines.append(f"{row['symbol']:<10} {str(row['name'])[:18]:<18} {cap_b:<10.2f} {price:<10.4f}")
            
            # Топ по Шарпу
            if not metrics_df.empty and 'sharpe_90d' in metrics_df:
                report_lines.extend([
                    "\n💎 ЛИДЕРЫ ПО КОЭФФИЦИЕНТУ ШАРПА (Эффективность):", 
                    "-" * 65,
                    f"{'Symbol':<10} {'Sharpe':<10} {'Vol (30d)':<12} {'Return (7d)':<12}"
                ])
                top_sharpe = metrics_df.sort_values('sharpe_90d', ascending=False).head(5)
                for _, row in top_sharpe.iterrows():
                    vol = row.get('volatility_30d', 0)
                    ret = row.get('return_7d', 0)
                    report_lines.append(f"{row['symbol']:<10} {row['sharpe_90d']:<10.2f} {vol:<12.2%} {ret:<12.2%}")

            # --- НОВОЕ: Отчет по On-Chain ---
            if onchain_df is not None and not onchain_df.empty:
                report_lines.extend(["\n🏗️ ЛИДЕРЫ РАЗРАБОТКИ (Developer Score):", "-" * 65])
                
                # Проверяем, есть ли колонка developer_score
                if 'developer_score' in onchain_df.columns:
                    top_dev = onchain_df.sort_values('developer_score', ascending=False).head(5)
                    for _, row in top_dev.iterrows():
                        symbol = row.get('symbol', 'UNK')
                        score = row.get('developer_score', 0)
                        report_lines.append(f"{symbol:<10} Dev Score: {score:.1f}")
                else:
                    report_lines.append("(Нет данных о разработчиках)")

            report_lines.append("=" * 60)
            
            report_text = "\n".join(report_lines)
            logger.info(report_text)
            
            # Сохранение
            report_dir = Config.BASE_DIR / "data" / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            
            with open(report_dir / "latest_report.txt", 'w', encoding='utf-8') as f:
                f.write(report_text)
                
        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}", exc_info=True)

def main():
    if not Config.DB_DIR.exists():
        Config.setup_directories()
    pipeline = CryptoAladdinPipeline()
    pipeline.run_full_pipeline()

if __name__ == "__main__":
    main()