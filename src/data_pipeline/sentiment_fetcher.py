import requests
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import logging

# --- НОВЫЕ ИМПОРТЫ ДЛЯ NLP ---
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from config.settings import Config
from src.utils.logger import logger

class SentimentFetcher:
    """Сбор новостей и AI-анализ настроений"""
    
    def __init__(self):
        self.session = requests.Session()
        self.panic_key = getattr(Config, 'CRYPTOPANIC_API_KEY', None)
        
        # Инициализация VADER (AI-анализатор)
        try:
            # Проверяем, скачан ли словарь
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            logger.info("📥 Скачивание словаря для анализа текста (NLTK)...")
            nltk.download('vader_lexicon', quiet=True)
        
        self.analyzer = SentimentIntensityAnalyzer()

    def fetch_fear_and_greed(self) -> Dict:
        """Получает индекс страха и жадности"""
        try:
            url = "https://api.alternative.me/fng/"
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            if data.get('data'):
                item = data['data'][0]
                return {
                    'value': int(item['value']),
                    'classification': item['value_classification'],
                    'date': datetime.now().date()
                }
        except Exception as e:
            logger.error(f"Ошибка получения Fear & Greed: {e}")
        
        return {'value': 50, 'classification': 'Neutral'}

    def analyze_text_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Оценивает тональность текста с помощью AI.
        Возвращает score (-1.0 до 1.0) и label (POS/NEG/NEUT).
        """
        if not text:
            return {'score': 0.0, 'label': 'NEUT'}
            
        # VADER выдает словарь: {'neg': 0.0, 'neu': 0.5, 'pos': 0.5, 'compound': 0.4}
        # compound - это общая нормализованная оценка
        scores = self.analyzer.polarity_scores(text)
        compound = scores['compound']
        
        # Определяем метку
        if compound >= 0.2:
            label = 'POS'  # Позитив
        elif compound <= -0.2:
            label = 'NEG'  # Негатив
        else:
            label = 'NEUT' # Нейтрально
            
        return {'score': compound, 'label': label}

    def fetch_news_for_coins(self, symbols: List[str]) -> List[Dict]:
        """
        Получает новости и проводит их анализ.
        """
        if not self.panic_key:
            return []
            
        news_items = []
        top_symbols = ",".join(symbols[:7]) # Берем топ-7 для запроса
        
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={self.panic_key}&currencies={top_symbols}&kind=news&filter=important"
        
        try:
            response = self.session.get(url, timeout=10)
            data = response.json()
            
            if 'results' in data:
                # Берем до 7 свежих важных новостей
                for post in data['results'][:7]:
                    title = post['title']
                    
                    # --- AI АНАЛИЗ ---
                    sentiment = self.analyze_text_sentiment(title)
                    
                    news_items.append({
                        'title': title,
                        'url': post['url'],
                        'published_at': post['published_at'],
                        'currencies': [c['code'] for c in post.get('currencies', []) if 'code' in c],
                        'sentiment_score': sentiment['score'],  # Цифра
                        'sentiment_label': sentiment['label']   # Метка
                    })
        except Exception as e:
            logger.error(f"Ошибка получения новостей: {e}")
            
        return news_items