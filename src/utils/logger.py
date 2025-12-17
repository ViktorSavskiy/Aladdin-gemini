import logging
import sys
from pathlib import Path
# ИЗМЕНЕНИЕ: Импортируем класс Config, а не отдельные переменные
from config.settings import Config

def setup_logger(name="crypto_aladdin"):
    """
    Настройка и получение логгера.
    """
    
    # Получаем логгер
    logger = logging.getLogger(name)
    
    # Если обработчики уже есть, не добавляем их снова (защита от дублирования)
    if logger.hasHandlers():
        return logger
    
    # 1. Настройка уровня логирования
    # ИЗМЕНЕНИЕ: Берем LOG_LEVEL из Config
    log_level_str = getattr(Config, 'LOG_LEVEL', 'INFO')
    try:
        level = getattr(logging, log_level_str.upper())
    except AttributeError:
        level = logging.INFO
        
    logger.setLevel(level)
    logger.propagate = False
    
    # 2. Форматтер
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 3. Создаем директорию для логов
    # ИЗМЕНЕНИЕ: Берем LOG_FILE из Config
    try:
        log_path = Config.LOG_FILE
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 4. Обработчик файла
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Внимание: Не удалось создать файл логов. Ошибка: {e}")

    # 5. Обработчик консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# Создаем глобальный экземпляр
logger = setup_logger()