import json
import logging
from typing import Dict, Any, List
from pathlib import Path

# Безопасный импорт PyYAML
try:
    import yaml
except ImportError:
    yaml = None

logger = logging.getLogger(__name__)

class StrategyLoader:
    """
    Класс для загрузки и управления весовыми коэффициентами стратегий.
    Синхронизирован с FactorCalculator.
    """
    
    # Ключи словаря должны совпадать с колонками, которые генерирует FactorCalculator!
    DEFAULT_STRATEGIES = {
        'balanced': {
            'name': 'Сбалансированный (Classic)',
            'description': 'Классический портфель: Рост + Качество + Низкий риск.',
            'weights': {
                'momentum_30d': 0.25,
                'quality_sharpe': 0.20,
                'low_volatility': 0.15,
                'size_large': 0.10,
                'category_advantage': 0.10, # НОВОЕ: Бонус за "модные" сектора
                'tvl_strength': 0.10,       # НОВОЕ: Бонус за реальные деньги в протоколе
                'correlation_low': 0.10
            }
        },
        'defi_value': {
            'name': 'Smart DeFi (Value)',
            'description': 'Поиск недооцененных DeFi проектов с высоким TVL.',
            'weights': {
                'defi_value': 0.35,        # Mcap/TVL Ratio (Главный фактор)
                'tvl_strength': 0.20,      # Абсолютный TVL
                'quality_dev': 0.15,       # Активность разработчиков
                'quality_sharpe': 0.15,    # Эффективность
                'momentum_30d': 0.15       # Технический тренд
            }
        },
        'short_speculative': {
            'name': 'Шорт (Спекулятивный)',
            'description': 'Поиск переоцененных активов с падающим трендом.',
            'weights': {
                'momentum_7d_bearish': 0.40,
                'high_volatility': 0.20,
                'value_nvt': 0.20,           # Высокий NVT = Переоценен
                'quality_sharpe': -0.20      # Штраф за хорошее качество
            }
        }
    }
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.strategies = self.DEFAULT_STRATEGIES.copy()
        
        # Проверяем наличие PyYAML при инициализации, если передан путь
        if config_path and (config_path.endswith('.yaml') or config_path.endswith('.yml')) and yaml is None:
            logger.warning("PyYAML не установлен. Загрузка .yaml файлов невозможна. (pip install PyYAML)")
        
        if config_path and Path(config_path).exists():
            self.load_custom_strategies(config_path)
    
    def load_custom_strategies(self, config_path: str):
        """Загрузка пользовательских стратегий из файла"""
        try:
            path = Path(config_path)
            data = {}
            
            with open(path, 'r', encoding='utf-8') as f:
                if path.suffix == '.json':
                    data = json.load(f)
                elif path.suffix in ['.yaml', '.yml'] and yaml:
                    data = yaml.safe_load(f)
                else:
                    logger.warning(f"Неподдерживаемый формат файла: {path.suffix}")
                    return
            
            # Валидация перед добавлением
            valid_strategies = {}
            for name, strat in data.items():
                if self.validate_strategy_weights(strat):
                    valid_strategies[name] = strat
            
            self.strategies.update(valid_strategies)
            logger.info(f"Загружено {len(valid_strategies)} пользовательских стратегий")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки стратегий из {config_path}: {e}")
    
    def get_strategy(self, strategy_name: str) -> Dict[str, Any]:
        """Получение стратегии по имени"""
        # Если стратегии нет, возвращаем дефолтную (balanced)
        if strategy_name not in self.strategies:
            logger.warning(f"Стратегия '{strategy_name}' не найдена. Используем 'balanced'")
            return self.strategies['balanced']
        
        return self.strategies[strategy_name]
    
    def validate_strategy_weights(self, strategy: Dict[str, Any]) -> bool:
        """Нормализация весов (сумма должна быть 1.0)"""
        if 'weights' not in strategy:
            logger.error("В стратегии отсутствует ключ 'weights'")
            return False
        
        weights = strategy['weights']
        
        # Убираем веса = 0, чтобы не засорять вычисления
        weights = {k: v for k, v in weights.items() if v != 0}
        
        total_weight = sum(abs(v) for v in weights.values()) # Используем abs, так как веса могут быть отрицательными (штрафы)
        
        if total_weight == 0:
            logger.error("Сумма весов равна 0")
            return False

        # Если сумма не равна 1 (с погрешностью), нормализуем
        if abs(total_weight - 1.0) > 0.001:
            logger.debug(f"Нормализация весов стратегии (было {total_weight:.2f})")
            for key in weights:
                weights[key] = weights[key] / total_weight
        
        strategy['weights'] = weights
        return True
    
    def get_active_factors(self, strategy_name: str) -> List[str]:
        """Возвращает список факторов, используемых в стратегии"""
        strat = self.get_strategy(strategy_name)
        return list(strat.get('weights', {}).keys())