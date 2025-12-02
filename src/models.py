from sklearn.ensemble import RandomForestRegressor
import joblib
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from .config import MODELS_DIR

class RatingPredictor:
    def __init__(self):
        self.model = None
        self.feature_names = None
    
    def train(self, X, y):
        """Обучает модель на данных"""
        self.feature_names = X.columns.tolist()
        
        # Настройки для баланса скорости и качества
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=15,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1,
            verbose=1
        )
        print("Начало обучения модели...")
        self.model.fit(X, y)
        print("Обучение завершено!")
        return self
    
    def predict(self, X):
        """Делает предсказания"""
        # Проверка соответствия признаков
        if self.feature_names:
            X = X.reindex(columns=self.feature_names, fill_value=0)
        
        print("Генерация предсказаний...")
        predictions = self.model.predict(X)
        return np.clip(predictions, 0, 10)  # Ограничение в диапазон [0,10]
    
    def save(self, filename="model.pkl"):
        """Сохраняет модель"""
        path = MODELS_DIR / filename
        joblib.dump(self, path)
        print(f"Модель сохранена: {path.absolute()}")
        return path
    
    @staticmethod
    def load(filename="model.pkl"):
        """Загружает модель"""
        path = MODELS_DIR / filename
        print(f"Загрузка модели из: {path.absolute()}")
        return joblib.load(path)