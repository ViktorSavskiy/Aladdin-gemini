"""
Модуль для сбора специфичных метрик (TVL, L1/L2 Stats) и классификации.
Использует DefiLlama как основной источник правды.
"""
import pandas as pd
import requests
import time
# --- ИСПРАВЛЕНИЕ: Добавлен импорт datetime ---
from datetime import datetime 
from typing import Dict, List, Optional, Any
import logging

from config.settings import Config
from src.utils.logger import logger

class CategoryFetcher:
    """Класс для умной классификации и сбора данных через DefiLlama"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CryptoAladdin/2.0'})
        
        # Кэш для справочников DefiLlama
        self.protocols_cache = None
        self.chains_cache = None
        
        # Правила классификации
        self.categories = Config.BLOCKCHAIN_CATEGORIES

    # --- 1. Работа с DefiLlama ---
    
    def _load_defillama_cache(self):
        """Загружает справочники протоколов и чейнов один раз"""
        if self.protocols_cache is not None:
            return

        logger.info("📥 Загрузка справочников DefiLlama...")
        try:
            # 1. Протоколы (Apps)
            resp = self.session.get("https://api.llama.fi/protocols", timeout=30)
            if resp.status_code == 200:
                self.protocols_cache = pd.DataFrame(resp.json())
            
            # 2. Чейны (L1/L2)
            resp = self.session.get("https://api.llama.fi/v2/chains", timeout=30)
            if resp.status_code == 200:
                self.chains_cache = pd.DataFrame(resp.json())
                
        except Exception as e:
            logger.error(f"Ошибка загрузки DefiLlama: {e}")
            self.protocols_cache = pd.DataFrame()
            self.chains_cache = pd.DataFrame()

    def fetch_defillama_stats(self, gecko_id: str) -> Dict[str, float]:
        """Ищет данные в DefiLlama по CoinGecko ID"""
        self._load_defillama_cache()
        stats = {}
        
        # А. Проверяем, является ли это Чейном (L1/L2)
        if not self.chains_cache.empty and 'gecko_id' in self.chains_cache.columns:
            chain_match = self.chains_cache[self.chains_cache['gecko_id'] == gecko_id]
            if not chain_match.empty:
                row = chain_match.iloc[0]
                stats['tvl'] = row.get('tvl', 0)
                stats['is_chain'] = True
                return stats

        # Б. Проверяем, является ли это Протоколом (DeFi App)
        if not self.protocols_cache.empty and 'gecko_id' in self.protocols_cache.columns:
            proto_match = self.protocols_cache[self.protocols_cache['gecko_id'] == gecko_id]
            
            if not proto_match.empty:
                row = proto_match.nlargest(1, 'tvl').iloc[0]
                stats['tvl'] = row.get('tvl', 0)
                stats['mcap_llama'] = row.get('mcap', 0)
                stats['category_llama'] = row.get('category', 'Unknown')
                stats['is_protocol'] = True
                return stats
                
        return stats

    # --- 2. Логика классификации ---

    def determine_category(self, coin_id: str, name: str, symbol: str, llama_cat: str = None) -> str:
        """Определяет категорию (L1, L2, DeFi, Meme, Gaming)"""
        
        cid = coin_id.lower()
        
        # 1. Если DefiLlama уже сказала категорию
        if llama_cat:
            if llama_cat == 'Chain': return 'L1'
            if llama_cat in ['Dexes', 'Lending', 'Yield', 'Derivatives', 'Liquid Staking']: return 'DeFi'
            if llama_cat == 'Gaming': return 'Gaming'
        
        # 2. Проверка по спискам из Config
        for cat, coins in self.categories.items():
            if cid in coins: return cat
            
        # 3. Эвристики
        meme_keywords = ['dog', 'shib', 'pepe', 'floki', 'meme', 'bonk', 'wif', 'trump']
        if any(k in cid for k in meme_keywords): return 'Meme'
        
        l2_keywords = ['optimism', 'arbitrum', 'base', 'mantle', 'starknet', 'zk', 'rollup']
        if any(k in cid for k in l2_keywords): return 'L2'

        return 'L1'

    # --- 3. Главный метод ---

    def fetch_specific_metrics(self, coin_list: List[Dict]) -> pd.DataFrame:
        """Сбор специфичных метрик для списка монет."""
        logger.info(f"🔎 Сбор специфичных метрик (TVL/Категории) для {len(coin_list)} монет...")
        
        results = []
        
        for i, coin in enumerate(coin_list):
            coin_id = coin.get('coin_id')
            symbol = coin.get('symbol')
            
            # 1. Запрос к DefiLlama
            llama_stats = self.fetch_defillama_stats(coin_id)
            
            # 2. Определение категории
            cat = self.determine_category(
                coin_id, 
                coin.get('name', ''), 
                symbol, 
                llama_stats.get('category_llama')
            )
            
            # 3. Собираем строку данных
            row = {
                'coin_id': coin_id,
                # Теперь datetime импортирован, ошибка исчезнет
                'date': datetime.now().date(),
                'category_type': cat,
                'tvl': llama_stats.get('tvl', 0)
            }
            
            # 4. Рассчитываем специфичные метрики
            mcap = coin.get('market_cap', 0)
            if row['tvl'] > 0 and mcap > 0:
                row['tvl_ratio'] = mcap / row['tvl']
            
            results.append(row)
            
            if i % 20 == 0:
                time.sleep(0.1)
                
        return pd.DataFrame(results)