import pandas as pd
import csv
import chardet
import re
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

from .config import DATA_DIR

def detect_file_format(file_path):
    """
    Анализирует файл и определяет правильные параметры для чтения CSV
    """
    print(f"  Анализ формата файла: {file_path.name}")
    
    # Читаем первые 10 строк для анализа
    sample_lines = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for _ in range(10):
            line = f.readline()
            if not line:
                break
            sample_lines.append(line.strip())
    
    if not sample_lines:
        raise ValueError("Файл пустой")
    
    print(f"    Пример первой строки: '{sample_lines[0]}'")
    
    # Анализируем разделитель
    possible_separators = [';', ',', '\t', '|']
    separator_counts = {}
    
    for sep in possible_separators:
        counts = [line.count(sep) for line in sample_lines]
        separator_counts[sep] = min(counts) if counts else 0
    
    # Выбираем разделитель с максимальным количеством вхождений
    detected_sep = max(separator_counts, key=separator_counts.get)
    sep_count = separator_counts[detected_sep]
    
    print(f"    Обнаружен разделитель: '{detected_sep}' (встречается {sep_count} раз в строке)")
    
    # Проверяем наличие кавычек
    quote_count = sum(1 for line in sample_lines for char in line if char == '"')
    use_quotes = quote_count > 10  # Если много кавычек, вероятно есть экранирование
    
    print(f"    Кавычек в образце: {quote_count} -> {'используем экранирование' if use_quotes else 'без экранирования'}")
    
    return {
        'sep': detected_sep,
        'quotechar': '"' if use_quotes else None,
        'quoting': csv.QUOTE_MINIMAL if use_quotes else csv.QUOTE_NONE
    }

def safe_read_csv(file_path):
    """Надежное чтение CSV файла с автоматическим анализом формата"""
    print(f"\n📥 Загрузка файла: {file_path.name}")
    
    # Определяем параметры формата файла
    format_params = detect_file_format(file_path)
    
    # Определяем кодировку
    try:
        with open(file_path, 'rb') as f:
            encoding_result = chardet.detect(f.read(100000))
        encoding = encoding_result['encoding'] or 'utf-8'
        print(f"    Определена кодировка: {encoding} (confidence: {encoding_result['confidence']:.2f})")
    except:
        encoding = 'utf-8'
        print(f"    Используем кодировку по умолчанию: {encoding}")
    
    # Пытаемся загрузить с правильными параметрами
    attempts = [
        # Попытка 1: C-движок с обнаруженными параметрами
        {
            'engine': 'c',
            'sep': format_params['sep'],
            'quotechar': format_params['quotechar'],
            'quoting': format_params['quoting'],
            'encoding': encoding,
            'on_bad_lines': 'warn',
            'low_memory': False
        },
        # Попытка 2: Python-движок (более гибкий)
        {
            'engine': 'python',
            'sep': format_params['sep'],
            'quotechar': format_params['quotechar'],
            'quoting': format_params['quoting'],
            'encoding': encoding,
            'on_bad_lines': 'skip'
        },
        # Попытка 3: Без экранирования (если кавычки мешают)
        {
            'engine': 'python',
            'sep': format_params['sep'],
            'quotechar': None,
            'quoting': csv.QUOTE_NONE,
            'encoding': encoding,
            'on_bad_lines': 'skip'
        }
    ]
    
    for i, params in enumerate(attempts, 1):
        try:
            print(f"    🔄 Попытка {i}: engine={params['engine']}, sep='{params['sep']}', encoding={encoding}")
            
            # Удаляем None-параметры
            clean_params = {k: v for k, v in params.items() if v is not None}
            
            df = pd.read_csv(file_path, **clean_params)
            
            # Проверяем, что загружено больше 1 столбца
            if len(df.columns) <= 1:
                raise ValueError(f"Файл загружен как один столбец. Столбцы: {df.columns.tolist()}")
            
            print(f"    ✅ Успешно! Строк: {len(df):,}, Столбцов: {len(df.columns)}")
            print(f"       Столбцы: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            
            # Выводим пример данных для отладки
            if not df.empty:
                print("       Пример данных:")
                print(df.head(2).to_string(index=False))
            
            return df
            
        except Exception as e:
            print(f"    ❌ Попытка {i} не удалась: {str(e)[:100]}...")
            continue
    
    # Финальная попытка: ручное разделение строк
    print("    ⚠️ Все автоматические попытки провалились. Пробуем ручное разделение...")
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            lines = f.readlines()
        
        if not lines:
            raise ValueError("Файл пустой")
        
        # Определяем заголовок и данные
        headers = lines[0].strip().split(format_params['sep'])
        data_rows = []
        
        for line in lines[1:]:
            row = line.strip().split(format_params['sep'])
            if len(row) == len(headers):
                data_rows.append(row)
        
        df = pd.DataFrame(data_rows, columns=headers)
        print(f"    ✅ Ручная загрузка успешна! Строк: {len(df):,}, Столбцов: {len(df.columns)}")
        return df
        
    except Exception as e:
        print(f"    ❌ Ручная загрузка не удалась: {str(e)[:100]}...")
        raise ValueError(f"Не удалось загрузить файл {file_path.name} ни одним из методов")

def load_data():
    """Загружает все данные в словарь с надежной обработкой форматов"""
    print("\n📚 ЗАГРУЗКА И АНАЛИЗ ДАННЫХ")
    data = {}
    
    # Список файлов для загрузки
    files_to_load = {
        "train": "train.csv",
        "test": "test.csv",
        "books": "books.csv",
        "users": "users.csv",
        "genres": "genres.csv",
        "book_genres": "book_genres.csv",
        "book_descriptions": "book_descriptions.csv"
    }
    
    for key, filename in files_to_load.items():
        file_path = DATA_DIR / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"❌ Файл не найден: {file_path.absolute()}")
        
        print(f"\n{'='*50}")
        print(f"📄 Работа с файлом: {filename}")
        print(f"{'='*50}")
        
        try:
            data[key] = safe_read_csv(file_path)
        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при загрузке {filename}:")
            print(f"    {str(e)}")
            print("\n💡 РЕКОМЕНДАЦИИ ПО ИСПРАВЛЕНИЮ:")
            print("1. Откройте файл в текстовом редакторе (Notepad++, VS Code)")
            print("2. Проверьте разделитель (должна быть точка с запятой ';')")
            print("3. Убедитесь, что первая строка содержит заголовки")
            print("4. Удалите пустые строки в конце файла")
            print("5. Сохраните файл в кодировке UTF-8 без BOM")
            raise
    
    # Фильтруем train только на прочитанные книги для обучения
    print(f"\n{'='*50}")
    print("🧹 ФИЛЬТРАЦИЯ ОБУЧАЮЩИХ ДАННЫХ")
    print(f"{'='*50}")
    
    print("  Проверка наличия необходимых столбцов в train.csv...")
    required_columns = ['user_id', 'book_id', 'rating', 'has_read']
    missing_cols = [col for col in required_columns if col not in data["train"].columns]
    
    if missing_cols:
        print(f"❌ ОТСУТСТВУЮЩИЕ СТОЛБЦЫ: {', '.join(missing_cols)}")
        print("  Столбцы в файле:", ', '.join(data["train"].columns))
        raise KeyError(f"В train.csv отсутствуют необходимые столбцы: {missing_cols}")
    
    initial_count = len(data["train"])
    data["train"] = data["train"][data["train"]["has_read"] == 1].copy()
    filtered_count = len(data["train"])
    
    print(f"  До фильтрации: {initial_count:,} строк")
    print(f"  После фильтрации (только has_read=1): {filtered_count:,} строк")
    print(f"  Удалено: {initial_count - filtered_count:,} строк ({(initial_count - filtered_count)/initial_count:.1%})")
    
    print(f"\n{'='*50}")
    print("✅ ЗАГРУЗКА ДАННЫХ УСПЕШНО ЗАВЕРШЕНА!")
    print(f"{'='*50}")
    
    return data