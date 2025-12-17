import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

from config.settings import Config
from src.utils.logger import logger

class DatabaseHandler:
    """
    Класс для работы с SQLite базой данных.
    Реализует безопасное сохранение (Upsert) и чтение данных.
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
                
                # --- Базовые таблицы (Market, History, Metrics, Filtered) ---
                self._create_base_tables(conn)
                
                # --- On-Chain таблицы (НОВОЕ) ---
                self._create_onchain_tables(conn)
                
                conn.commit()
        except Exception as e:
            logger.error(f"Critical DB Init Error: {e}")
            raise

    def _create_base_tables(self, conn):
        """Создание основных таблиц"""
        # Market Data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS market_data (
                coin_id TEXT NOT NULL,
                symbol TEXT,
                name TEXT, 
                date DATE NOT NULL,
                price REAL,
                market_cap REAL,
                volume_24h REAL,
                change_24h REAL,
                change_7d REAL,
                change_30d REAL,
                timestamp DATETIME,
                last_updated DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))
        
        # Historical Data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS historical_data (
                coin_id TEXT NOT NULL,
                date DATE NOT NULL,
                price REAL,
                volume REAL,
                PRIMARY KEY (coin_id, date)
            )
        """))
        
        # Metrics
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS metrics (
                coin_id TEXT NOT NULL,
                calculation_date DATE NOT NULL,
                symbol TEXT,
                price REAL,
                market_cap REAL,
                volatility_30d REAL,
                sharpe_90d REAL,
                max_drawdown_365d REAL,
                correlation_btc REAL,
                beta_btc REAL,
                return_7d REAL,
                return_30d REAL,
                data_days INTEGER,
                last_updated DATETIME,
                PRIMARY KEY (coin_id, calculation_date)
            )
        """))
        
        # Filtered Assets
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS filtered_assets (
                coin_id TEXT NOT NULL,
                date DATE NOT NULL,
                symbol TEXT,
                category TEXT,
                market_cap REAL,
                PRIMARY KEY (coin_id, date)
            )
        """))

    def _create_onchain_tables(self, conn):
        """Создание таблиц для on-chain данных"""
        # Основная таблица метрик
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS onchain_metrics (
                coin_id TEXT NOT NULL,
                symbol TEXT,
                date DATE NOT NULL,
                
                -- Messari
                messari_active_addresses REAL,
                messari_transaction_volume REAL,
                messari_transaction_count REAL,
                messari_transaction_fees REAL,
                
                -- CoinGecko Dev Stats
                coingecko_forks INTEGER,
                coingecko_stars INTEGER,
                coingecko_subscribers INTEGER,
                coingecko_total_issues INTEGER,
                coingecko_closed_issues INTEGER,
                coingecko_commit_count_4_weeks INTEGER,
                
                -- Расчетные
                estimated_active_addresses REAL,
                developer_score REAL,
                
                -- Scores
                score_network_activity REAL,
                score_transaction_volume REAL,
                score_total_onchain_score REAL,
                
                last_updated DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))
        
        # Daily Snapshot (рейтинг)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS onchain_daily_snapshot (
                coin_id TEXT NOT NULL,
                symbol TEXT,
                date DATE NOT NULL,
                
                onchain_health_score REAL,
                network_activity_score REAL,
                economic_activity_score REAL,
                development_score REAL,
                
                ranking_position INTEGER,
                percentile REAL,
                
                timestamp DATETIME,
                PRIMARY KEY (coin_id, date)
            )
        """))

    def _upsert_data(self, df: pd.DataFrame, table_name: str):
        """Универсальный Upsert (Insert or Replace)"""
        if df.empty:
            return

        # Генерируем уникальное имя временной таблицы
        temp_table = f"temp_{table_name}_{datetime.now().strftime('%M%S%f')}"
        
        with self.engine.begin() as conn:
            try:
                # 1. Пишем во временную таблицу
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                
                # 2. Формируем список колонок
                columns = df.columns.tolist()
                cols_str = ", ".join(columns)
                
                # 3. Upsert в основную таблицу
                sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {temp_table}"
                conn.execute(text(sql))
                
                # 4. Удаляем временную
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                
                logger.info(f"Upsert в {table_name}: обработано {len(df)} строк")
                
            except Exception as e:
                logger.error(f"Ошибка Upsert в {table_name}: {e}")
                # Пробрасываем ошибку, чтобы видеть её в логах main.py
                raise

    # --- Методы сохранения (Standard) ---
    
    def save_market_data(self, df: pd.DataFrame):
        if 'timestamp' in df.columns and 'date' not in df.columns:
             df['date'] = df['timestamp'].dt.date
        self._upsert_data(df, 'market_data')

    def save_historical_data(self, historical_data: Dict[str, pd.DataFrame]):
        if not historical_data: return
        all_dfs = []
        for coin_id, df in historical_data.items():
            df_copy = df.copy()
            df_copy['coin_id'] = coin_id
            if 'date' in df_copy.columns:
                df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date
            all_dfs.append(df_copy)
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            keep_cols = ['coin_id', 'date', 'price', 'volume']
            combined_df = combined_df[[c for c in keep_cols if c in combined_df.columns]]
            self._upsert_data(combined_df, 'historical_data')

    def save_metrics(self, df: pd.DataFrame):
        if df.empty: return
        df = df.copy()
        if 'calculation_date' in df.columns:
            df['calculation_date'] = pd.to_datetime(df['calculation_date']).dt.date
        else:
            df['calculation_date'] = datetime.now().date()
        self._upsert_data(df, 'metrics')

    def save_filtered_assets(self, df: pd.DataFrame):
        if df.empty: return
        df = df.copy()
        df['date'] = datetime.now().date()
        keep_cols = ['coin_id', 'date', 'symbol', 'category', 'market_cap']
        df = df[[c for c in keep_cols if c in df.columns]]
        self._upsert_data(df, 'filtered_assets')

    # --- Методы сохранения (On-Chain) ---

    def save_onchain_data(self, onchain_df: pd.DataFrame):
        """Сохранение on-chain данных с расчетом снепшота"""
        if onchain_df.empty:
            logger.warning("Пустой DataFrame on-chain данных")
            return
        
        try:
            # 1. Сохраняем метрики (Upsert)
            # Приводим дату к правильному типу
            if 'date' in onchain_df.columns:
                onchain_df['date'] = pd.to_datetime(onchain_df['date']).dt.date
                
            self._upsert_data(onchain_df, 'onchain_metrics')
            
            # 2. Создаем и сохраняем Snapshot
            self._create_daily_snapshot(onchain_df)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения On-Chain данных: {e}")

    def _create_daily_snapshot(self, onchain_df: pd.DataFrame):
        """Внутренний метод для расчета рейтингов"""
        try:
            snapshot_data = []
            
            # Если в onchain_df нет нужных колонок Score, ставим 0
            for _, row in onchain_df.iterrows():
                total_score = row.get('score_total_onchain_score', 0)
                
                snapshot = {
                    'coin_id': row['coin_id'],
                    'symbol': row.get('symbol', ''),
                    'date': row['date'],
                    'timestamp': datetime.now(),
                    'onchain_health_score': total_score,
                    # Примерная логика разбиения скоров (если они есть)
                    'network_activity_score': row.get('score_network_activity', 0),
                    'development_score': row.get('developer_score', 0) 
                }
                snapshot_data.append(snapshot)
            
            if not snapshot_data:
                return

            snapshot_df = pd.DataFrame(snapshot_data)
            
            # Считаем Ранг и Процентиль
            if 'onchain_health_score' in snapshot_df.columns:
                snapshot_df['ranking_position'] = snapshot_df['onchain_health_score'].rank(
                    ascending=False, method='min'
                ).astype(int)
                
                snapshot_df['percentile'] = snapshot_df['onchain_health_score'].rank(pct=True) * 100
            
            # Сохраняем Snapshot (Upsert)
            self._upsert_data(snapshot_df, 'onchain_daily_snapshot')
            
        except Exception as e:
            logger.error(f"Ошибка создания Daily Snapshot: {e}")

    # --- Методы чтения ---

    def get_latest_onchain_data(self, days: int = 7) -> pd.DataFrame:
        try:
            # Используем параметры для защиты от инъекций
            cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            query = """
                SELECT * FROM onchain_metrics 
                WHERE date >= :cutoff
                ORDER BY date DESC, score_total_onchain_score DESC
            """
            return pd.read_sql_query(query, self.engine, params={'cutoff': cutoff})
        except Exception as e:
            logger.error(f"Ошибка чтения On-Chain: {e}")
            return pd.DataFrame()

    def cleanup_old_data(self, days_to_keep: int = 365):
        try:
            cutoff = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
            with self.engine.begin() as conn:
                tables = [
                    ('market_data', 'date'), 
                    ('historical_data', 'date'), 
                    ('metrics', 'calculation_date'), 
                    ('filtered_assets', 'date'),
                    ('onchain_metrics', 'date'),      # + Новая таблица
                    ('onchain_daily_snapshot', 'date') # + Новая таблица
                ]
                for table, date_col in tables:
                    conn.execute(text(f"DELETE FROM {table} WHERE {date_col} < :cutoff"), {'cutoff': cutoff})
        except Exception as e:
            logger.error(f"Ошибка очистки данных: {e}")