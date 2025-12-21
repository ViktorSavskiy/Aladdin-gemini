"""
Модуль для сбора on-chain метрик криптовалют.
"""
import pandas as pd
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import Config
from src.utils.logger import logger

class OnChainFetcher:
    """Класс для получения фундаментальных метрик блокчейна"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CryptoAladdin/1.0 (On-Chain)',
            'Accept': 'application/json'
        })
        
        # Настройка источников
        self.sources = {
            'messari': {
                'enabled': Config.ONCHAIN_SOURCES.get('messari', False),
                'base_url': 'https://data.messari.io/api/v1',
                'api_key': Config.MESSARI_API_KEY
            },
            'glassnode': {
                'enabled': Config.ONCHAIN_SOURCES.get('glassnode', False),
                'base_url': 'https://api.glassnode.com/v1',
                'api_key': Config.GLASSNODE_API_KEY
            },
            'coingecko': {
                'enabled': Config.ONCHAIN_SOURCES.get('coingecko_onchain', True),
                'base_url': 'https://api.coingecko.com/api/v3',
                'api_key': Config.COINGECKO_API_KEY
            }
        }
        
        if self.sources['coingecko']['api_key']:
             # Правильный заголовок для CoinGecko Pro API
             self.session.headers.update({'x-cg-pro-api-key': self.sources['coingecko']['api_key']})

        # --- ИСПРАВЛЕНИЕ: Автоматический расчет задержки ---
        # Берем лимит из настроек (например, 5). 60 / 5 = 12 секунд.
        # Добавляем запас +2 секунды.
        limit = Config.API_RATE_LIMITS.get('coingecko', 5) 
        self.delay = (60 / limit) + 2.0
        logger.info(f"OnChain задержка установлена на {self.delay:.1f} сек")

    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def _make_request(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code == 429:
                logger.warning(f"OnChain Rate Limit (429). Ждем 65 секунд...")
                time.sleep(65) # Длинная пауза для снятия бана
                raise requests.exceptions.RequestException("Rate Limit Hit")
                
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                return None
            raise e

    def fetch_messari_metrics(self, symbol: str) -> Dict[str, float]:
        if not self.sources['messari']['enabled']: return {}
        
        # Messari лимиты тоже жесткие, поэтому если нет ключа, лучше пропустить или делать редко
        headers = {}
        if self.sources['messari']['api_key']:
            headers['x-messari-api-key'] = self.sources['messari']['api_key']

        url = f"{self.sources['messari']['base_url']}/assets/{symbol}/metrics"
        try:
            data = self._make_request(url, headers=headers)
            if not data or 'data' not in data: return {}
            
            onchain = data['data'].get('blockchain_stats_24_hours', {})
            results = {}
            results['transaction_volume'] = onchain.get('transaction_volume', 0)
            results['transaction_count'] = onchain.get('count_of_tx', 0)
            results['active_addresses'] = onchain.get('count_of_active_addresses', 0)
            
            mining = data['data'].get('mining_stats', {})
            if mining: results['hash_rate'] = mining.get('hash_rate', 0)
            return results
        except:
            return {}

    def fetch_coingecko_dev_stats(self, coin_id: str) -> Dict[str, float]:
        """Получение Developer Score и сырых данных с CoinGecko"""
        if not self.sources['coingecko']['enabled']: return {}
        
        url = f"{self.sources['coingecko']['base_url']}/coins/{coin_id}"
        params = {
            'localization': 'false', 'tickers': 'false', 
            'market_data': 'false', 'community_data': 'false', 
            'developer_data': 'true', 'sparkline': 'false'
        }
        
        try:
            data = self._make_request(url, params=params)
            if not data: return {}
            
            dev = data.get('developer_data', {})
            
            # 1. Считаем общий балл (как раньше)
            score = (
                dev.get('forks', 0) * 2 +
                dev.get('stars', 0) * 0.5 +
                dev.get('commit_count_4_weeks', 0) * 5 +
                dev.get('pull_requests_merged', 0) * 3
            )
            
            # 2. Возвращаем и балл, и все доступные детали
            result = {
                'developer_score': score,
                'coingecko_stars': dev.get('stars', 0),
                'coingecko_forks': dev.get('forks', 0),
                'coingecko_commit_count_4_weeks': dev.get('commit_count_4_weeks', 0),
                'coingecko_pull_requests_merged': dev.get('pull_requests_merged', 0),
            }
            
            # Добавляем дополнительные поля, если они доступны
            if 'subscribers' in dev:
                result['coingecko_subscribers'] = dev.get('subscribers', 0)
            if 'total_issues' in dev:
                result['coingecko_total_issues'] = dev.get('total_issues', 0)
            if 'closed_issues' in dev:
                result['coingecko_closed_issues'] = dev.get('closed_issues', 0)
            if 'pull_request_contributors' in dev:
                result['coingecko_pull_request_contributors'] = dev.get('pull_request_contributors', 0)
            
            return result
            
        except Exception as e:
            # logger.debug(f"Dev stats error: {e}") # Можно раскомментировать для отладки
            return {}

    def fetch_all_onchain_data(self, coin_list: List[Dict]) -> pd.DataFrame:
        logger.info(f"🧬 Сбор On-Chain метрик. Пауза между монетами: {self.delay:.1f} сек...")
        results = []
        
        for i, coin in enumerate(coin_list, 1):
            coin_id = coin.get('coin_id')
            symbol = coin.get('symbol')
            
            row = {'coin_id': coin_id, 'symbol': symbol, 'date': datetime.now().date()}
            
            # 1. Messari
            messari = self.fetch_messari_metrics(symbol)
            if messari:
                row['messari_active_addresses'] = messari.get('active_addresses')
                row['messari_transaction_volume'] = messari.get('transaction_volume')
                row['messari_transaction_count'] = messari.get('transaction_count')
            
            # 2. CoinGecko Developer Stats
            dev_data = self.fetch_coingecko_dev_stats(coin_id)
            if dev_data:
                # Обновляем строку всеми полученными полями
                row.update(dev_data)

            clean_row = {k: v for k, v in row.items() if v is not None}
            results.append(clean_row)
            
            # --- ИСПРАВЛЕНИЕ: ИСПОЛЬЗУЕМ РАСЧЕТНУЮ ЗАДЕРЖКУ ---
            time.sleep(self.delay) 
            
            if i % 5 == 0:
                logger.info(f"   Прогресс On-Chain: {i}/{len(coin_list)}")

        return pd.DataFrame(results)
