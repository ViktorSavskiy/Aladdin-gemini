import sys
import pandas as pd # <--- Добавлен импорт
from pathlib import Path

# Добавляем корневую директорию в PYTHONPATH, чтобы Python видел пакеты src и config
# Это позволяет запускать файл как 'python main.py'
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
    Главный оркестратор: Сбор -> Фильтрация -> История -> Метрики -> База -> Отчет
    """
    
    def __init__(self):
        self.fetcher = DataFetcher()
        self.filter = DataFilter()
        self.processor = DataProcessor()
        self.db_handler = DatabaseHandler() # Инициализация DB происходит внутри __init__
        
    def _ensure_btc_history(self, historical_data: dict, coin_ids: list) -> dict:
        """
        Гарантирует наличие истории BTC для расчета корреляции.
        Если BTC нет в отфильтрованном списке, загружает его отдельно.
        """
        btc_id = 'bitcoin'
        if btc_id not in historical_data:
            logger.info("BTC отсутствует в списке активов. Загружаем историю BTC для корреляции...")
            btc_data = self.fetcher.fetch_historical_data(btc_id, days=Config.HISTORICAL_DAYS)
            if not btc_data.empty:
                historical_data[btc_id] = btc_data
            else:
                logger.warning("Не удалось загрузить историю BTC! Корреляция будет NaN.")
        return historical_data

    def run_full_pipeline(self):
        """Запуск полного цикла обновления данных"""
        try:
            logger.info("=" * 60)
            logger.info("🚀 ЗАПУСК CRYPTO ALADDIN PIPELINE")
            logger.info("=" * 60)
            
            # --- Шаг 1: Рыночные данные ---
            logger.info("[1/6] Получение текущих рыночных данных...")
            market_data = self.fetcher.fetch_coingecko_market_data()
            
            if market_data.empty:
                logger.error("❌ Остановка: Не удалось получить рыночные данные.")
                return
            
            self.db_handler.save_market_data(market_data)
            
            # --- Шаг 2: Фильтрация ---
            logger.info("[2/6] Фильтрация и категоризация активов...")
            # apply_all_filters уже включает категоризацию и удаление стейблов (если настроено)
            filtered_data = self.filter.apply_all_filters(market_data, exclude_stables=True)
            
            if filtered_data.empty:
                logger.error("❌ Остановка: Нет активов, прошедших фильтры.")
                return
                
            self.db_handler.save_filtered_assets(filtered_data)
            
            # --- Шаг 3: История цен ---
            coin_ids = filtered_data['coin_id'].tolist()
            logger.info(f"[3/6] Сбор истории для {len(coin_ids)} активов (это займет время)...")
            
            historical_data = self.fetcher.fetch_all_historical_data(
                coin_ids, 
                days=Config.HISTORICAL_DAYS
            )
            
            # ВАЖНО: Проверяем наличие BTC для расчетов
            historical_data = self._ensure_btc_history(historical_data, coin_ids)
            
            self.db_handler.save_historical_data(historical_data)
            
            # --- Шаг 4: Расчет метрик ---
            logger.info("[4/6] Расчет финансовых метрик (Volatility, Sharpe, Beta)...")
            metrics_df = self.processor.calculate_all_metrics(
                historical_data, 
                market_data # Передаем полные данные, чтобы подтянуть имена и символы
            )
            
            self.db_handler.save_metrics(metrics_df)
            
            # --- Шаг 5: Очистка старого ---
            logger.info("[5/6] Очистка устаревших данных из БД...")
            self.db_handler.cleanup_old_data(days_to_keep=365)
            
            # --- Шаг 6: Отчет ---
            logger.info("[6/6] Генерация отчета...")
            self.generate_report(metrics_df, filtered_data)
            
            logger.info("=" * 60)
            logger.info("✅ ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН")
            logger.info("=" * 60)
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Процесс остановлен пользователем.")
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в пайплайне: {e}", exc_info=True)
    
    def generate_report(self, metrics_df: pd.DataFrame, filtered_data: pd.DataFrame):
        """Генерация текстового отчета"""
        try:
            timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            
            report_lines = [
                "\n" + "=" * 60,
                f"ОТЧЕТ CRYPTO ALADDIN | {timestamp}",
                "=" * 60,
                f"Всего проанализировано: {len(filtered_data)} активов",
                f"Успешно рассчитано:     {len(metrics_df)} активов",
                "\n🏆 ТОП-10 АКТИВОВ ПО КАПИТАЛИЗАЦИИ (из выборки):",
                "-" * 50,
                f"{'Symbol':<10} {'Name':<20} {'Cap ($B)':<10} {'Price ($)':<10}"
            ]
            
            # Топ-10
            # Сортируем filtered_data, так как в metrics_df могут быть не все (если история не загрузилась)
            top_10 = filtered_data.sort_values('market_cap', ascending=False).head(10)
            
            for _, row in top_10.iterrows():
                cap_b = row.get('market_cap', 0) / 1e9
                price = row.get('price', 0)
                report_lines.append(f"{row['symbol']:<10} {str(row['name'])[:18]:<20} {cap_b:<10.2f} {price:<10.4f}")
            
            # Статистика по метрикам
            if not metrics_df.empty:
                # Проверяем наличие колонок перед доступом (защита от KeyError)
                vol = metrics_df['volatility_30d'].mean() if 'volatility_30d' in metrics_df else 0
                ret7 = metrics_df['return_7d'].mean() if 'return_7d' in metrics_df else 0
                # В DataProcessor мы назвали колонку correlation_btc, а не correlation_btc_30d
                corr_col = 'correlation_btc' if 'correlation_btc' in metrics_df else 'correlation_btc_30d'
                corr = metrics_df[corr_col].median() if corr_col in metrics_df else 0
                
                report_lines.extend([
                    "\n📈 СРЕДНИЕ ПОКАЗАТЕЛИ РЫНКА:",
                    "-" * 50,
                    f"Волатильность (30d):   {vol:.2%}",
                    f"Доходность (7d):       {ret7:.2%}",
                    f"Корреляция с BTC:      {corr:.2f}",
                ])
                
                # Топ по Шарпу (самые эффективные)
                if 'sharpe_90d' in metrics_df:
                    report_lines.extend(["\n💎 ЛИДЕРЫ ПО КОЭФФИЦИЕНТУ ШАРПА (Risk/Reward):", "-" * 50])
                    top_sharpe = metrics_df.sort_values('sharpe_90d', ascending=False).head(5)
                    for _, row in top_sharpe.iterrows():
                        report_lines.append(f"{row['symbol']:<10} Sharpe: {row['sharpe_90d']:.2f} | Vol: {row['volatility_30d']:.2f}")

            report_lines.append("=" * 60)
            
            report_text = "\n".join(report_lines)
            logger.info(report_text) # Вывод в консоль
            
            # Сохраняем в файл, используя Config.BASE_DIR
            report_dir = Config.BASE_DIR / "data" / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            
            file_name = f"report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.txt"
            report_path = report_dir / file_name
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            
            # Сохраняем также как latest
            with open(report_dir / "latest_report.txt", 'w', encoding='utf-8') as f:
                f.write(report_text)
                
            logger.info(f"📄 Отчет сохранен: {report_path}")
            
        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}", exc_info=True)

def main():
    """Точка входа"""
    # Проверка, создан ли конфиг
    if not Config.DB_DIR.exists():
        Config.setup_directories()
        
    pipeline = CryptoAladdinPipeline()
    pipeline.run_full_pipeline()

if __name__ == "__main__":
    main()