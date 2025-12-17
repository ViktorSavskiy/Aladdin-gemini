import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List, Dict
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
        """Создание таблиц, если они не существуют"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA journal_mode=WAL;"))
                
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
                
                # Metrics (ИСПРАВЛЕНО: price вместо current_price, добавлены новые поля)
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
                
                conn.commit()
        except Exception as e:
            logger.error(f"Critical DB Init Error: {e}")
            raise

    def _upsert_data(self, df: pd.DataFrame, table_name: str):
        if df.empty:
            return

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
                # Если ошибка в структуре таблицы, выводим подробности
                raise

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

    # --- Методы чтения (без изменений) ---
    def cleanup_old_data(self, days_to_keep: int = 365):
        try:
            cutoff = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
            with self.engine.begin() as conn:
                tables = [('market_data', 'date'), ('historical_data', 'date'), 
                         ('metrics', 'calculation_date'), ('filtered_assets', 'date')]
                for table, date_col in tables:
                    conn.execute(text(f"DELETE FROM {table} WHERE {date_col} < :cutoff"), {'cutoff': cutoff})
        except Exception as e:
            logger.error(f"Ошибка очистки данных: {e}")