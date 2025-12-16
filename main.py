

# Импортируем необходимые модули
from modules.data_collector import DataCollector
import pandas as pd
import logging

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Alladin")

def main():
    """Основная функция для запуска DataCollector"""
    
    # Создаем экземпляр DataCollector
    logger.info("Создание DataCollector...")
    collector = DataCollector()
    
    # Получаем данные
    logger.info("Запрос данных с Binance...")
    
    # Параметры запроса
    symbol = "BTC/USDT"      # Торговая пара
    timeframe = "1h"         # Таймфрейм (1m, 5m, 15m, 1h, 4h, 1d и т.д.)
    limit = 100              # Количество свечей
    
    try:
        # Вызываем метод fetch_ohlcv
        df = collector.fetch_ohlcv(symbol, timeframe, limit)
        
        # Проверяем результат
        if not df.empty:
            logger.info(f"Успешно получено {len(df)} свечей")
            print("\n=== Первые 5 строк данных ===")
            print(df.head())
            
            # Сохраняем в CSV для анализа
            csv_path = os.path.join(project_path, "btc_usdt_data.csv")
            df.to_csv(csv_path, index=False)
            logger.info(f"Данные сохранены в {csv_path}")
            
            # Показываем базовую статистику
            print("\n=== Базовая статистика ===")
            print(f"Период: с {df['timestamp'].min()} по {df['timestamp'].max()}")
            print(f"Средняя цена закрытия: {df['close'].mean():.2f}")
            
        else:
            logger.warning("Получен пустой DataFrame")
            
    except Exception as e:
        logger.error(f"Ошибка в основном потоке: {e}")

if __name__ == "__main__":
    main()

