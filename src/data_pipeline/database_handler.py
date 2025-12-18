import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

from config.settings import Config
from src.utils.logger import logger

class DatabaseHandler:
    """
    Класс для работы с SQLite базой данных.
    Включает: Создание таблиц, Upsert данных, Чтение данных.
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DB_PATH
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        self._init_db()

    def _init_db(self):
        """Инициализация всех таблиц"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL;"))
                
                self._create_base_tables(conn)
                self._create_onchain_tables(conn)
                self._create_score_tables(conn)
                self._create_category_tables(conn) # <--- НОВОЕ
                
                conn.commit()
        except Exception as e:
            logger.error(f"Critical DB Init Error: {e}")
            raise

    # --- СОЗДАНИЕ ТАБЛИЦ ---

    def _create_base_tables(self, conn):
        # ... (код market_data, historical_data, metrics, filtered_assets без изменений) ...
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS market_data (
                coin_id TEXT NOT NULL, symbol TEXT, name TEXT, date DATE NOT NULL,
                price REAL, market_cap REAL, volume_24h REAL, 
                change_24h REAL, change_7d REAL, change_30d REAL,
                timestamp DATETIME, last_updated DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS historical_data (
                coin_id TEXT NOT NULL, date DATE NOT NULL,
                price REAL, volume REAL,
                PRIMARY KEY (coin_id, date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS metrics (
                coin_id TEXT NOT NULL, calculation_date DATE NOT NULL, symbol TEXT,
                price REAL, market_cap REAL, volatility_30d REAL, sharpe_90d REAL,
                max_drawdown_365d REAL, correlation_btc REAL, beta_btc REAL,
                return_7d REAL, return_30d REAL, data_days INTEGER, last_updated DATETIME,
                PRIMARY KEY (coin_id, calculation_date)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS filtered_assets (
                coin_id TEXT NOT NULL, date DATE NOT NULL,
                symbol TEXT, category TEXT, market_cap REAL,
                PRIMARY KEY (coin_id, date)
            )
        """))

    def _create_onchain_tables(self, conn):
        # ... (код onchain_metrics, onchain_daily_snapshot без изменений) ...
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS onchain_metrics (
                coin_id TEXT NOT NULL, symbol TEXT, date DATE NOT NULL,
                messari_active_addresses REAL, messari_transaction_volume REAL,
                messari_transaction_count REAL,
                developer_score REAL,
                last_updated DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))

    def _create_score_tables(self, conn):
        # ... (код asset_ranks без изменений) ...
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS asset_ranks (
                coin_id TEXT NOT NULL, symbol TEXT, date DATE NOT NULL,
                net_score REAL, long_score REAL, short_score REAL,
                final_rank INTEGER, signal TEXT, primary_driver TEXT,
                timestamp DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))

    # --- НОВОЕ: ТАБЛИЦЫ КАТЕГОРИЙ ---
    def _create_category_tables(self, conn):
        """Создание таблиц для категорий и специфичных метрик"""
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS asset_categories (
                coin_id TEXT NOT NULL,
                symbol TEXT,
                date DATE NOT NULL,
                category TEXT NOT NULL,
                
                -- Специфичные метрики (DeFi/L1/L2)
                tvl REAL,
                tvl_ratio REAL,
                
                -- Метаданные
                category_score REAL,
                calculated_at DATETIME,
                
                PRIMARY KEY (coin_id, date)
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS category_stats (
                date DATE NOT NULL,
                category TEXT NOT NULL,
                asset_count INTEGER,
                total_tvl REAL,
                timestamp DATETIME,
                
                PRIMARY KEY (date, category)
            )
        """))

    # --- UPSERT (УНИВЕРСАЛЬНЫЙ) ---

    def _upsert_data(self, df: pd.DataFrame, table_name: str):
        if df.empty: return
        
        # Обработка сложных типов (dict/list) в JSON строку перед записью
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Если в ячейке словарь или список, превращаем в JSON строку
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                except Exception:
                    pass

        temp_table = f"temp_{table_name}_{datetime.now().strftime('%M%S%f')}"
        with self.engine.begin() as conn:
            try:
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                columns = df.columns.tolist()
                cols_str = ", ".join(columns)
                sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {temp_table}"
                conn.execute(text(sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                logger.info(f"Upsert в {table_name}: обработано {len(df)} строк")
            except Exception as e:
                logger.error(f"Ошибка Upsert в {table_name}: {e}")
                # Если таблицы нет или структура не совпадает, можно попробовать пересоздать (опционально)
                raise

    # --- МЕТОДЫ СОХРАНЕНИЯ (BASE) ---
    def save_market_data(self, df: pd.DataFrame):
        if 'timestamp' in df.columns and 'date' not in df.columns: df['date'] = df['timestamp'].dt.date
        self._upsert_data(df, 'market_data')

    def save_historical_data(self, historical_data: Dict[str, pd.DataFrame]):
        if not historical_data: return
        all_dfs = []
        for coin_id, df in historical_data.items():
            df_copy = df.copy()
            df_copy['coin_id'] = coin_id
            if 'date' in df_copy.columns: df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
            all_dfs.append(df_copy)
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            self._upsert_data(combined[['coin_id', 'date', 'price', 'volume']], 'historical_data')

    def save_metrics(self, df: pd.DataFrame):
        if df.empty: return
        df = df.copy()
        if 'calculation_date' in df.columns: df['calculation_date'] = pd.to_datetime(df['calculation_date']).dt.date
        else: df['calculation_date'] = datetime.now().date()
        self._upsert_data(df, 'metrics')

    def save_filtered_assets(self, df: pd.DataFrame):
        if df.empty: return
        df = df.copy()
        df['date'] = datetime.now().date()
        self._upsert_data(df[['coin_id', 'date', 'symbol', 'category', 'market_cap']], 'filtered_assets')

    def save_onchain_data(self, onchain_df: pd.DataFrame):
        if onchain_df.empty: return
        if 'date' in onchain_df.columns: onchain_df['date'] = pd.to_datetime(onchain_df['date']).dt.date
        self._upsert_data(onchain_df, 'onchain_metrics')

    def save_scores(self, scores_df: pd.DataFrame):
        if scores_df.empty: return
        df = scores_df.copy()
        df['date'] = datetime.now().date()
        df['timestamp'] = datetime.now()
        cols = ['coin_id', 'symbol', 'date', 'timestamp', 'net_score', 'long_score', 'short_score', 'final_rank', 'signal', 'primary_driver']
        for c in cols: 
            if c not in df.columns: df[c] = None
        self._upsert_data(df[cols], 'asset_ranks')

    # --- НОВОЕ: СОХРАНЕНИЕ КАТЕГОРИЙ ---

    def save_category_data(self, category_df: pd.DataFrame):
        """Сохранение данных категорий (с безопасным Upsert)"""
        if category_df.empty: return
        
        try:
            df = category_df.copy()
            
            # Приводим дату к правильному формату
            if 'date' not in df.columns:
                df['date'] = datetime.now().date()
            else:
                df['date'] = pd.to_datetime(df['date']).dt.date
                
            df['calculated_at'] = datetime.now()
            
            # Маппинг имен (если SpecificFetcher вернул category_type, а база ждет category)
            if 'category_type' in df.columns and 'category' not in df.columns:
                df = df.rename(columns={'category_type': 'category'})

            # Сохраняем основную таблицу
            # Оставляем только те колонки, которые есть в базе (упрощенно)
            valid_cols = [
                'coin_id', 'symbol', 'date', 'category', 'tvl', 'tvl_ratio', 
                'category_score', 'calculated_at'
            ]
            # Фильтруем, оставляя только существующие в DF
            cols_to_save = [c for c in valid_cols if c in df.columns]
            
            self._upsert_data(df[cols_to_save], 'asset_categories')
            
            # Создаем статистику
            self._create_category_stats(df)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения категорий: {e}")

    def _create_category_stats(self, category_df: pd.DataFrame):
        """Создание статистики по категориям"""
        try:
            stats_data = []
            
            # Группируем по категории
            for category, group in category_df.groupby('category'):
                stats = {
                    'date': datetime.now().date(),
                    'category': category,
                    'asset_count': len(group),
                    'total_tvl': group['tvl'].sum() if 'tvl' in group.columns else 0,
                    'timestamp': datetime.now()
                }
                stats_data.append(stats)
            
            if stats_data:
                stats_df = pd.DataFrame(stats_data)
                self._upsert_data(stats_df, 'category_stats')
                
        except Exception as e:
            logger.error(f"Ошибка статистики категорий: {e}")

    # --- МЕТОДЫ ЧТЕНИЯ ---

    def get_latest_metrics(self) -> pd.DataFrame:
        try:
            return pd.read_sql_query("SELECT * FROM metrics WHERE calculation_date = (SELECT MAX(calculation_date) FROM metrics)", self.engine)
        except: return pd.DataFrame()

    def get_latest_onchain_data(self, days: int = 1) -> pd.DataFrame:
        try:
            return pd.read_sql_query("SELECT * FROM onchain_metrics WHERE date = (SELECT MAX(date) FROM onchain_metrics)", self.engine)
        except: return pd.DataFrame()

    def get_filtered_assets(self) -> pd.DataFrame:
        try:
            return pd.read_sql_query("SELECT * FROM filtered_assets WHERE date = (SELECT MAX(date) FROM filtered_assets)", self.engine)
        except: return pd.DataFrame()

    def cleanup_old_data(self, days_to_keep: int = 365):
        try:
            cutoff = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
            with self.engine.begin() as conn:
                tables = ['market_data', 'historical_data', 'metrics', 'filtered_assets', 'onchain_metrics', 'asset_ranks', 'asset_categories', 'category_stats']
                col_map = {'metrics': 'calculation_date'}
                for t in tables:
                    col = col_map.get(t, 'date')
                    conn.execute(text(f"DELETE FROM {t} WHERE {col} < :cutoff"), {'cutoff': cutoff})
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")