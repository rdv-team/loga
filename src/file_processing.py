# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

# =============================================================================
# ИМПОРТЫ
# =============================================================================
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Generator, Dict, Any, List, Set
from filters import FilterSet

# =============================================================================
# КОНСТАНТЫ И ПЕРЕМЕННЫЕ
# =============================================================================
logger = logging.getLogger(__name__)

# Регулярные выражения и кэш паттернов
# Базовый паттерн для начала строки события
NEW_LINE_PATTERN = r'^(\d{2}):(\d{2})\.\d+-\d+,'

# Скомпилированные паттерны
PATTERN_NEW_EVENT = re.compile(NEW_LINE_PATTERN)
PATTERN_EXTRACT_EVENT = re.compile(r'^\d{2}:\d{2}\.\d+-(\d+),[A-Za-z]+,(.+)')

# Предкомпилированные паттерны для замены кавычек
PATTERN_DOUBLE_QUOTES = re.compile(r'""')
PATTERN_SINGLE_QUOTES = re.compile(r"''")

# Кэш для скомпилированных регулярных выражений
_EVENT_PATTERN_CACHE = {}
_KEYS_PATTERN_CACHE = {}

# =============================================================================
# УТИЛИТАРНЫЕ ФУНКЦИИ
# =============================================================================
def format_line(line: str, add_newline: bool = False) -> str:
    """
    Обработка строки с заменой парных кавычек с помощью регулярных выражений.
    Форматирует строку лога, заменяя парные кавычки на специальные маркеры и
    опционально добавляя маркер новой строки для многострочных событий.
    
    Args:
        line: Исходная строка лога
        add_newline: Добавлять ли маркер новой строки для продолжения события
    
    Returns:
        str: Отформатированная строка лога с замененными кавычками
    """
    # Сначала делаем strip() для удаления пробельных символов
    processed = line.strip()
    
    # Заменяем пары кавычек используя предкомпилированные регулярные выражения
    processed = PATTERN_DOUBLE_QUOTES.sub('<DQ>', processed)
    processed = PATTERN_SINGLE_QUOTES.sub('<OQ>', processed)
    
    return "<_NL_>" + processed if add_newline else processed

def get_event_pattern(event_name: str) -> re.Pattern:
    """
    Получает скомпилированный паттерн для события из кэша или создает новый.
    
    Args:
        event_name: Имя события для поиска
    
    Returns:
        re.Pattern: Скомпилированный регулярный паттерн
    """
    if event_name not in _EVENT_PATTERN_CACHE:
        _EVENT_PATTERN_CACHE[event_name] = re.compile(
            NEW_LINE_PATTERN + re.escape(event_name) + r','
        )
    return _EVENT_PATTERN_CACHE[event_name]

def get_keys_pattern(extraction_keys: Set[str]) -> re.Pattern:
    """
    Получает скомпилированный паттерн для набора ключей из кэша или создает новый.
    
    Args:
        extraction_keys: Набор ключей для поиска
    
    Returns:
        re.Pattern: Скомпилированный регулярный паттерн
    """
    # Создаем стабильный ключ для кэша, сортируя ключи
    cache_key = '|'.join(sorted(extraction_keys))
    
    if cache_key not in _KEYS_PATTERN_CACHE:
        keys_pattern = '|'.join(map(re.escape, extraction_keys))
        _KEYS_PATTERN_CACHE[cache_key] = re.compile(
            rf',({keys_pattern})=(?:"([^"]*)"|\'([^\']*)\'|([^,\r\n]+))'
        )
    return _KEYS_PATTERN_CACHE[cache_key]

def parse_timestamp(file_date: str, minute: str, second: str) -> datetime:
    """
    Парсинг времени события из строки ТЖ и имени файла.
    
    Args:
        file_date: Дата из имени файла в формате YYMMDD
        minute: Минуты из строки события
        second: Секунды из строки события
    
    Returns:
        datetime: Объект datetime
    """
    try:
        year = 2000 + int(file_date[0:2])
        month = int(file_date[2:4])
        day = int(file_date[4:6])
        hour = int(file_date[6:8])
        minute = int(minute)
        second = int(second)
        return datetime(year, month, day, hour, minute, second)
    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка парсинга timestamp: {file_date}{minute}{second} - {e}")
        raise

def read_log_file(file_path: Path, encoding: str) -> Generator[str, None, None]:
    """
    Простое построчное чтение файла.
    
    Args:
        file_path: Путь к файлу
        encoding: Кодировка файла
    
    Yields:
        str: Строка из файла без символа новой строки
    """
    with open(file_path, 'r', encoding=encoding, errors='replace') as file:
        for line in file:
            yield line.rstrip('\r\n')

def replace_literals_and_temp_tables(query: str) -> str:
    """
    Замена параметров в SQL-запросе.
    """
    regex_patterns = [
        r"('\\[^']*'::bytea)",  # Байтовые строки с любыми символами
        r"('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'::\w+)",  # Даты с типом данных (например, timestamp)
        r"('\w*')",  # Строковые литералы в одинарных кавычках
        r"\b\d+\.\d+\b",  # Числа с плавающей запятой
        r"\b\d+\b",  # Целые числа
        r"\bpg_temp\.\w+\b",  # Временные таблицы, например pg_temp.tt28
        r"VALUES\s*\(([^)]+)\)",  # Значения в выражении VALUES
    ]
    combined_regex = "|".join(regex_patterns)

    def replace_match(match):
        if "pg_temp" in match.group(0):
            return "pg_temp_table"
        return "_PARAM_"

    return re.sub(combined_regex, replace_match, query)

# =============================================================================
# ФУНКЦИИ ПАРСИНГА ДАННЫХ
# =============================================================================
def parse_log_line(
    log_line: str,
    extraction_keys: Set[str],
    logger
) -> Dict[str, Any]:
    """Парсинг строки лога и извлечение базовых данных."""

    """Проверка соответствия шаблону строки ТЖ"""  
    match = PATTERN_EXTRACT_EVENT.match(log_line)
    if not match:
        return {}
    
    """Проверка наличия всех нужных свойств в стркое ТЖ""" 
    if not all(f",{key}=" in log_line for key in extraction_keys):
        return {}
    
    duration, key_values = match.groups()
    
    event_data = {}
    # Инициализируем все ожидаемые ключи пустыми значениями
    for key in sorted(extraction_keys):
        event_data[key] = None

    try:
        event_data['duration'] = int(duration) / 1000000
    except ValueError as e:
        logger.error(f"Ошибка преобразования длительности: {duration} - {e}")
        return {}
    
    # Получаем скомпилированный паттерн для ключей из кэша
    combined_pattern = get_keys_pattern(extraction_keys)

    for match in combined_pattern.findall(key_values):
        key, v1, v2, v3 = match
        value = (v1 or v2 or v3).strip()
        if key == 'Sql':
            value = replace_literals_and_temp_tables(value)
        value = value.replace('<DQ>', '""').replace("<OQ>", "''")

        # Попытка преобразования значения в число
        try:
            num_val = float(value)
            value = int(num_val) if num_val.is_integer() else num_val
        except ValueError:
            pass

        event_data[key] = value

    return event_data

def apply_transformations(
    event_data: Dict[str, Any],
    substitution_rules: List[Dict],
    original_keys: Set[str],
    logger
) -> Dict[str, Any]:
    """Применение правил преобразования и замены ключей."""
    for rule in substitution_rules:
        target, source, transform = rule["target"], rule["source"], rule["transform"]

        if source in event_data and target in original_keys:
            try:
                event_data[target] = transform(event_data[source])
            except Exception as e:
                logger.error(f"Ошибка преобразования значения для {target}: {event_data[source]} - {e}")

    return event_data

def extract_event_data(
    buffer_parts: List[str],
    event_name: str,
    timestamp_dt: datetime,
    keys_list: str,
    filter_set: FilterSet,
    logger
) -> List[Dict[str, Any]]:
    """Основная функция извлечения события из буфера."""
    log_line = "".join(buffer_parts)

    base_event_data = {
        'timestamp': timestamp_dt,
        'Name': event_name,
        'count': 1,
        'duration': 0
    }

    # Правила преобразования
    substitution_rules = [
        {
            "target": "lastcontext",
            "source": "Context",
            "transform": lambda v: v.split("<_NL_>")[-1]
        }
    ]

    keys_to_extract = keys_list.split(',')
    original_keys = set(keys_to_extract)

    # Извлекаем из события ключ-источник (например, Context)
    # После этого преобразовываем ключ-источник в ключ-приемник (Context->lastcontext) по правилу transform
    extraction_keys = set(keys_to_extract).difference(base_event_data)
    for rule in substitution_rules:
        if rule["target"] in extraction_keys:
            extraction_keys.discard(rule["target"])
            extraction_keys.add(rule["source"])
    
    event_data = parse_log_line(log_line, extraction_keys, logger)
    if not event_data:
        return []

    event_data.update({k: v for k, v in base_event_data.items() if k != 'duration'})

    event_data = apply_transformations(event_data, substitution_rules, original_keys, logger)

    # Заменяем <_NL_> на \n во всех строковых полях
    for key, value in event_data.items():
        if isinstance(value, str) and '<_NL_>' in value:
            event_data[key] = value.replace('<_NL_>', '\n')
            logger.debug(f"Заменены <_NL_> на \\n в поле {key}")

    if filter_set.matches_all(event_data):
        return [event_data]

    return []

# =============================================================================
# ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ ФАЙЛОВ
# =============================================================================
def preprocess_log_file_streaming(
    file_path: Path,
    event_name: str,
    start_period_dt: datetime,
    end_period_dt: datetime,
    keys_list: str,
    filter_set: FilterSet,
    encoding: str
):
    """
    Генератор, который построчно читает лог и выдаёт события по одному.
    """
    pattern_filter_event = get_event_pattern(event_name)
    file_date = file_path.stem[:8]
    buffer_parts = []
    timestamp_dt = None
    skip_lines = True

    for line in read_log_file(file_path, encoding):
        if PATTERN_NEW_EVENT.match(line):
            if buffer_parts:
                try:
                    extracted = extract_event_data(buffer_parts, event_name, timestamp_dt, keys_list, filter_set, logger)
                    for ev in extracted:
                        yield ev
                except Exception as e:
                    logger.warning(f"Ошибка при извлечении события: {e}")
                buffer_parts = []
                skip_lines = True

            match = pattern_filter_event.match(line)
            if not match:
                skip_lines = True
                continue

            minute, second = match.groups()
            try:
                timestamp_dt = parse_timestamp(file_date, minute, second)
            except ValueError as e:
                logger.error(f"Неверный формат времени: {line.strip()} - {e}")
                skip_lines = True
                continue

            if start_period_dt <= timestamp_dt <= end_period_dt:
                buffer_parts.append(format_line(line))
                skip_lines = False
            else:
                skip_lines = True
        elif not skip_lines:
            buffer_parts.append(format_line(line, add_newline=True))

    if buffer_parts:
        try:
            extracted = extract_event_data(buffer_parts, event_name, timestamp_dt, keys_list, filter_set, logger)
            for ev in extracted:
                yield ev
        except Exception as e:
            logger.warning(f"Ошибка при извлечении финального события: {e}")