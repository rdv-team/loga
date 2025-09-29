# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

# =============================================================================
# ИМПОРТЫ
# =============================================================================
import os
import re  
import time
import json
import shutil
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count
from typing import Dict, Any, List

# Импорт tqdm для прогресс-бара (опционально)
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None
    
# Сторонние
import polars as pl

# Локальные
from filters import load_filters
from file_processing import preprocess_log_file_streaming
from aggregation_registry import agg_registry, register_all_intervals_from_config

# =============================================================================
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# =============================================================================
logger = logging.getLogger(__name__)

# =============================================================================
# УТИЛИТАРНЫЕ ФУНКЦИИ
# =============================================================================
def detect_encoding_fallback(file_path: Path) -> str:
    """
    Определяет кодировку лог-файла с fallback: utf-8 → cp1251
    Логирует выбор кодировки.
    """
    encodings_to_try = ['utf-8-sig', 'utf-8', 'cp1251']

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(10000)
            logger.info(f"Определена кодировка '{encoding}' на основе файла {file_path.name}")
            return encoding
        except UnicodeDecodeError as e:
            logger.warning(f"Не удалось прочитать файл {file_path.name} с кодировкой '{encoding}': {e}")
            continue

    logger.error(f"Не удалось определить корректную кодировку для файла {file_path.name}, используется 'utf-8' по умолчанию")
    return 'utf-8'
    
def reorder_columns_by_keys_result(df: pl.DataFrame, keys_result: List[Dict]) -> pl.DataFrame:
    """
    Переупорядочивает колонки DataFrame в строгом порядке по keys_result.
    """
    ordered_columns = []

    for key in keys_result:
        key_name = key['name']
        aggregations = key.get('aggregation', [])
        metrics = key.get('metrics', [])

        # 1. Без агрегаций и метрик — просто колонка
        if not aggregations and not metrics:
            if key_name in df.columns:
                ordered_columns.append(key_name)
            continue

        # 2. Агрегации
        for agg_name in aggregations:
            agg = agg_registry.get(agg_name)
            if not agg:
                continue

            if agg.is_range:
                pattern = re.compile(rf"^{key_name}_{agg_name}(_(<\d+)|(_\d+-\d+)|(_\d+>))?$")
                intervals_cols = [col for col in df.columns if pattern.match(col)]
                ordered_columns.extend(intervals_cols)
            else:
                postfix = agg.postfix or ''
                column_name = f"{key_name}{postfix}"
                if column_name in df.columns:
                    ordered_columns.append(column_name)

    # 4. Вернуть только найденные колонки, без ошибки
    return df.select([col for col in ordered_columns if col in df.columns])

def rename_columns_by_keys_result(result_df: pl.DataFrame, keys_result: List[Dict]) -> pl.DataFrame:
    """
    Переименовывает колонки DataFrame на основе keys_result (alias, агрегации, метрики, range).
    Использует данные из agg_registry.

    Args:
        result_df (pl.DataFrame): Результирующий датафрейм.
        keys_result (List[Dict]): Конфигурация ключей.

    Returns:
        pl.DataFrame: DataFrame с переименованными колонками.
    """
    rename_dict = {}

    for key in keys_result:
        key_name = key["name"]
        alias = key.get("alias", key_name)
        rename_dict[key_name] = alias

        # Обработка агрегаций
        for agg_name in key.get("aggregation", []):
            agg = agg_registry.get(agg_name)
            if not agg:
                continue

            if agg.is_range:
                # Обработка intervals-агрегаций
                intervals_pattern = re.compile(rf"^{key_name}_{agg_name}(_(<\d+)|(_\d+-\d+)|(_\d+>))?$")
                for col in result_df.columns:
                    if intervals_pattern.match(col):
                        # Например: duration_intervals_5_5_10-15 → (10-15)
                        tail = col.rsplit("_", 1)[-1]
                        rename_dict[col] = f"{alias} ({tail})"
            else:
                postfix = agg.postfix or ''
                full_col = key_name + postfix
                if full_col in result_df.columns:
                    rename_dict[full_col] = f"{alias}{agg.display_name}"

    return result_df.rename(rename_dict) if rename_dict else result_df 

def build_polars_expression_from_formula(formula: str) -> pl.Expr:
    """
    Преобразует строку формулы в Polars выражение.
    Поддерживаются только простые математические операции и имена колонок.
    
    Args:
        formula: Строка с формулой (например, "duration / count")
        
    Returns:
        pl.Expr: Polars выражение
        
    Raises:
        Exception: При ошибке создания выражения
    """
    # Найти все имена колонок (слова, не являющиеся числами или операторами)
    tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", formula)
    expr = formula
    for token in set(tokens):
        expr = re.sub(rf"\b{token}\b", f"pl.col('{token}')", expr)
    
    # Создаем безопасное выражение Polars
    try:
        # Создаем локальное пространство имен с polars
        local_dict = {'pl': pl}
        # Выполняем выражение в безопасном контексте
        result = eval(expr, {"__builtins__": {}}, local_dict)
        # Округляем результат до 3 знаков
        return result.round(3)
    except Exception as e:
        logger.error(f"Ошибка при создании выражения из формулы '{formula}': {e}")
        logger.error(f"Преобразованное выражение: {expr}")
        raise

# =============================================================================
# КОНВЕЕР ОБРАБОТКИ ДАННЫХ
# =============================================================================

def apply_preprocessing_expressions(df: pl.DataFrame, keys_result: List[Dict]) -> pl.DataFrame:
    exprs = []

    for key in keys_result:
        col_name = key['name']
        for agg_name in key.get('aggregation', []):
            agg = agg_registry.get(agg_name)
            if agg and agg.pre_process_expr_fn:
                exprs.append(agg.pre_process_expr_fn(col_name))

    return df.with_columns(exprs) if exprs else df

def apply_pre_aggregation_expressions(
    df: pl.DataFrame,
    keys_result: List[Dict],
    group_keys: List[str]
) -> pl.DataFrame:
    """
    Выполняет предварительную агрегацию на DataFrame на основе конфигурации.

    Args:
        df (pl.DataFrame): Входной DataFrame.
        keys_result (List[Dict]): Конфигурация агрегаций.
        group_keys (List[str]): Ключи для группировки.

    Returns:
        pl.DataFrame: Агрегированный DataFrame.
    """
    agg_expressions = []
    processed_keys_cache = set()

    for key in keys_result:
        column_name = key["name"]

        if column_name in group_keys or column_name in processed_keys_cache:
            continue

        aggregations = key.get("aggregation", [])

        for agg_name in aggregations:
            agg = agg_registry.get(agg_name)
            if not agg:
                continue

            if agg.pre_agg_expr_fn:
                expr = agg.pre_agg_expr_fn(column_name)
                if isinstance(expr, list):
                    agg_expressions.extend(expr)
                else:
                    agg_expressions.append(expr)

        processed_keys_cache.add(column_name)

    if agg_expressions:
        # Удаляем дубликаты выражений по строковому представлению
        unique_exprs = {}
        for expr in agg_expressions:
            unique_exprs[str(expr)] = expr
        df = df.group_by(group_keys).agg(list(unique_exprs.values()))

    return df

def apply_final_aggregation(
    result_df: pl.LazyFrame,
    keys_result: List[Dict],
    group_keys: List[str]
) -> pl.LazyFrame:
    """
    Применяет только финальную агрегацию (final_agg_expr_fn) к сгруппированным данным,
    без постобработки (post_agg_expr_fn).
    
    Args:
        result_df (pl.LazyFrame): Входной датафрейм.
        keys_result (List[Dict]): Конфигурация ключей.
        group_keys (List[str]): Ключи группировки.

    Returns:
        pl.LazyFrame: Аггрегированный результат.
    """
    agg_expressions = []
    processed_keys_cache = set()

    for key in keys_result:
        column_name = key['name']
        if column_name in group_keys or column_name in processed_keys_cache:
            continue

        aggregations = key.get('aggregation', [])

        for agg_name in aggregations:
            agg = agg_registry.get(agg_name)
            if not agg:
                continue

            if agg.final_agg_expr_fn:
                expr = agg.final_agg_expr_fn(column_name)
                if isinstance(expr, list):
                    agg_expressions.extend(expr)
                else:
                    agg_expressions.append(expr)

        processed_keys_cache.add(column_name)

    if agg_expressions:
        # Удаляем дубликаты выражений по строковому представлению
        unique_exprs = {}
        for expr in agg_expressions:
            unique_exprs[str(expr)] = expr
        result_df = result_df.group_by(group_keys).agg(list(unique_exprs.values()))

    return result_df

def apply_postprocessing_expressions(result_df: pl.LazyFrame, keys_result: List[Dict]) -> pl.LazyFrame:
    """
    Выполняет постобработку с помощью post_process_expr_fn для агрегаций,
    которые требуют финального выражения с with_columns.

    Args:
        result_df (pl.LazyFrame): Агрегированный датафрейм.
        keys_result (List[Dict]): Конфигурация ключей.

    Returns:
        pl.LazyFrame: Датафрейм с результатами после post-обработки.
    """
    post_exprs = []
    drop_columns = []

    for key in keys_result:
        column_name = key['name']
        aggregations = key.get('aggregation', [])

        for agg_name in aggregations:
            agg = agg_registry.get(agg_name)
            if not agg or not agg.post_process_expr_fn:
                continue

            expr = agg.post_process_expr_fn(column_name)
            if isinstance(expr, list):
                post_exprs.extend(expr)
            else:
                post_exprs.append(expr)

            if agg.needs_raw_values:
                drop_columns.append(f"{column_name}_values")

            if agg.name == "mean":
                drop_columns.extend([
                    f"{column_name}_mean_sum",
                    f"{column_name}_mean_count"
                ])

    if post_exprs:
        result_df = result_df.with_columns(post_exprs)

    if drop_columns:
        result_df = result_df.drop(drop_columns, strict=False)

    return result_df

def apply_formulas(result_df: pl.LazyFrame, keys_result: List[Dict]) -> pl.LazyFrame:
    """
    Применяет формулы к DataFrame.
    
    Args:
        result_df: LazyFrame с данными
        keys_result: Список ключей из конфигурации
    
    Returns:
        pl.LazyFrame: LazyFrame с примененными формулами
    """
    formula_expressions = []
    
    for key in keys_result:
        if 'formula' in key:
            formula = key['formula']
            try:
                polars_expr = build_polars_expression_from_formula(formula).alias(key['name'])
                formula_expressions.append(polars_expr)
            except Exception as e:
                logger.error(f"Ошибка при разборе формулы '{formula}' для колонки '{key['name']}': {e}")
                raise
            logger.info(f"Выполнен расчет динамической колонки {key['name']} по формуле: {formula}")
    
    # Применяем формулы, если есть простые выражения
    if formula_expressions:
        result_df = result_df.with_columns(formula_expressions)
        logger.info(f"Применены формулы для {len(formula_expressions)} колонок")
    
    return result_df

# =============================================================================
# ФУНКЦИИ РАБОТЫ С ФАЙЛАМИ
# =============================================================================
def generate_column_batches(events, batch_size=100_000):
    """
    Генератор, который преобразует поток событий в batched dict[str, list] для Polars.
    """
    columns = {}
    count = 0

    for event in events:
        for k, v in event.items():
            columns.setdefault(k, []).append(v)
        count += 1

        if count >= batch_size:
            yield columns
            columns = {}
            count = 0

    if count > 0:
        yield columns

def process_file_to_parquet(args) -> str:
    """
    Обрабатывает большой лог-файл порциями и сохраняет каждый батч в отдельный Parquet-файл.
    Возвращает путь к каталогу с батчами или None, если данные отсутствуют.
    """
    try:
        file_path, common_args, temp_dir, aggregation_config = args
        
        group_keys = []
        keys_result = []
        if aggregation_config and aggregation_config.get('keys_aggregate'):
            group_keys = [k.strip() for k in aggregation_config['keys_aggregate'].split(',')]
            keys_result = aggregation_config.get('keys_result', [])

        register_all_intervals_from_config(keys_result)

        file_name = Path(file_path).stem
        batch_index = 0
        written_any = False
        batch_dir = None

        events_stream = preprocess_log_file_streaming(file_path, *common_args)

        for columns in generate_column_batches(events_stream, batch_size=100_000):
            if not columns:
                continue

            # Создаем директорию только при первой записи
            if batch_dir is None:
                batch_dir = Path(temp_dir) / f"{file_name}_batches_{os.getpid()}_{int(time.time() * 1000)}"
                batch_dir.mkdir(parents=True, exist_ok=True)

            df = pl.DataFrame(columns)

            df = apply_preprocessing_expressions(df, keys_result)
            
            # Применяем округление timestamp
            for key in keys_result:
                if "roundtime" in key:
                    roundtime = key["roundtime"]
                    match = re.fullmatch(r"(\d+)([smhd])", roundtime)
                    if not match:
                        raise ValueError(f"Неверный формат roundtime '{roundtime}'. Допустимые значения: '5s', '10m', '1h', '1d'")
                    value, unit = match.groups()
                    if int(value) <= 0:
                        raise ValueError(f"Значение интервала должно быть положительным: {roundtime}")
                    df = df.with_columns([
                        pl.col("timestamp").dt.round(roundtime).alias("timestamp")
                    ])

            if group_keys:
                df = apply_pre_aggregation_expressions(df, keys_result, group_keys)

            batch_path = batch_dir / f"{file_name}_batch_{batch_index}.parquet"
            df.write_parquet(batch_path)
            written_any = True
            batch_index += 1

        if not written_any:
            return None

        return str(batch_dir)

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {args[0]} в Parquet: {e}")
        return None

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================
def parse_logs_to_xlsx(config: Dict[str, Any]) -> float:
    """
    Основная функция для парсинга логов и сохранения результатов в Excel.
    
    Args:
        config: Конфигурационный объект с параметрами обработки
        
    Returns:
        float: Общий размер обработанных файлов в МБ
        
    Raises:
        KeyError: При отсутствии обязательных параметров в конфигурации
        ValueError: При неверном формате периода
        Exception: При ошибках обработки данных
    """
    start_time = time.time()

    # Извлечение параметров из config
    try:
        base_directories = config["base_directories"]
        base_subdirectories = config["base_subdirectories"] 
        output_directory = config["output_directory"]
        output_file = config["output_file"]
        start_period = config["start_period"]
        end_period = config["end_period"]
        event_name = config["event_name"]
        event_limit = config["event_limit"]
        keys_result = config["keys_result"]
        keys_filters = config["keys_filters"]
        keys_aggregate = config["keys_aggregate"]
        keys_sort = config["keys_sort"]
        cpu_usage_percent = config["cpu_usage_percent"]
        keep_temp_files = config.get("keep_temp_files", False)
        
        # Формируем keys_list из keys_result (только поля без formula)
        keys_list = ",".join([key['name'] for key in keys_result])
        
    except KeyError as e:
        logger.error(f"Отсутствует параметр в конфигурации: {e}")
        raise

    logger.info(
        "Начало запуска парсинга. Входные параметры (config):\n" +
        json.dumps(config, ensure_ascii=False, indent=4)
    )
    
    logger.info(f"Свойства событий журнала для извлечения сформированы: {keys_list}")

    try:
        start_period_dt = datetime.strptime(start_period, "%y%m%d%H%M%S")
        end_period_dt = datetime.strptime(end_period, "%y%m%d%H%M%S")
    except ValueError as e:
        logger.error(f"Неверный формат периода (start_period={start_period}, end_period={end_period}): {e}")
        raise

    # Загрузка фильтров из JSON-конфигурации
    try:
        filter_set = load_filters(keys_filters)
    except Exception as e:
        logger.error(f"Не удалось загрузить фильтры: {e}")
        raise

    logger.info(
        f"Событие: {event_name}, Диапазон: {start_period_dt.strftime('%d.%m.%y %H:%M:%S')} - {end_period_dt.strftime('%d.%m.%y %H:%M:%S')}"
    )

    log_files = []
    for directory in base_directories:
        directory_path = Path(directory.strip())

        # Проверяем наличие логов в самом базовом каталоге, если он совпадает с одним из base_subdirectories
        if directory_path.name in base_subdirectories:
            log_files.extend(directory_path.rglob('*.log'))

        # Рекурсивно ищем подкаталоги и проверяем их
        for subdir in directory_path.rglob('*'):
            if subdir.is_dir() and subdir.name in base_subdirectories:
                log_files.extend(subdir.rglob('*.log'))

    # Исключение файлов, которые не попадают в диапазон времени и имеют размер < 20 байт
    filtered_log_files = []
    for file in log_files:
        file_date_str = file.stem[:8]
        try:
            file_date_dt = datetime.strptime(file_date_str, "%y%m%d%H")
            if start_period_dt <= file_date_dt <= end_period_dt and file.stat().st_size > 20:
                filtered_log_files.append(file)
        except ValueError:
            logger.warning(f"Неверный формат даты в имени файла: {file}")
            continue
    log_files = filtered_log_files

    total_size_mb = sum(file.stat().st_size for file in log_files) / (1024 * 1024)
    logger.info(f"Общее количество файлов для парсинга: {len(log_files)}, Общий размер: {total_size_mb:.2f} МБ")

    all_events = []

    # Определяем кодировку по первому файлу
    if log_files:
        first_file = log_files[0]
        encoding = detect_encoding_fallback(first_file)
    else:
        logger.warning("Список файлов для обработки пуст. Завершение работы.")
        return total_size_mb
    
    # Создаем временную директорию для промежуточных Parquet файлов
    temp_dir = tempfile.mkdtemp(prefix="parser_1c_temp_")
    parquet_files = []
    
    logger.info(f"Создана временная директория: {temp_dir}")

    register_all_intervals_from_config(keys_result)

    # Запуск чтения и анализа файлов ТЖ с записью в Parquet
    if cpu_usage_percent == 0:
        logger.info("Запуск обработки файлов в один поток с записью в Parquet")
        
        common_args = (event_name, start_period_dt, end_period_dt, 
                      keys_list, filter_set, encoding)
        
        if tqdm is not None:
            total_size_mb = sum(f.stat().st_size for f in log_files) / (1024 * 1024)
            processed_size = 0
            
            with tqdm(total=len(log_files), desc="Обработка файлов", unit="файл") as pbar:
                for file in log_files:
                    parquet_path = process_file_to_parquet((file, common_args, temp_dir, config))
                    if parquet_path:
                        parquet_files.append(parquet_path)
                    pbar.update(1)
        else:
            # Простая обработка без прогресс-бара
            for file in log_files:
                parquet_path = process_file_to_parquet((file, common_args, temp_dir, config))
                if parquet_path:
                    parquet_files.append(parquet_path)
    else:
        processes = max(1, min(cpu_count(), int(cpu_count() * (cpu_usage_percent / 100))))
        logger.info(f"Запуск многопоточной обработки файлов с записью в Parquet. Количество процессов: {processes}")
        
        # Общие аргументы для всех файлов
        common_args = (event_name, start_period_dt, end_period_dt, 
                      keys_list, filter_set, encoding)
        
        # Подготовка аргументов с временной директорией
        process_args = [(file, common_args, temp_dir, config) for file in log_files]
        
        # Запуск пула процессов с прогресс-баром
        pool = Pool(processes=processes)
        try:
            # Используем imap_unordered для прогресс-бара
            parquet_files = []
            if tqdm is not None:
                with tqdm(total=len(process_args), desc="Обработка файлов", unit="файл") as pbar:
                    for result in pool.imap_unordered(process_file_to_parquet, process_args):
                        if result is not None:
                            parquet_files.append(result)
                        pbar.update(1)
            else:
                # Простая обработка без прогресс-бара
                for result in pool.imap_unordered(process_file_to_parquet, process_args):
                    if result is not None:
                        parquet_files.append(result)
        except Exception as e:
            logger.error(f"Ошибка при многопоточной обработке: {e}")
            raise
        finally:
            pool.close()
            pool.join()

    if not parquet_files:
        logger.warning("Не найдено событий для обработки.")
        return total_size_mb

    logger.info(f"Создано {len(parquet_files)} промежуточных Parquet файлов")
    
    # Объединяем все Parquet файлы в один DataFrame
    logger.info("Объединение промежуточных Parquet файлов...")
    
    # Используем ленивое чтение и объединение для оптимизации памяти
    lazy_dfs = [pl.scan_parquet(parquet_file) for parquet_file in parquet_files]
    result_df = pl.concat(lazy_dfs, how="vertical")
    
    logger.info(f"Запланировано создание DataFrame из {len(parquet_files)} файлов")
    
    # Если есть ключи группировки, выполняем финальную агрегацию
    if keys_aggregate.strip():
        group_keys = [k.strip() for k in keys_aggregate.split(',')]
        
        # Применяем финальную агрегацию
        result_df = apply_final_aggregation(result_df, keys_result, group_keys)

        logger.info(f"Запланирована финальная агрегация по ключам '{keys_aggregate}'")

        # Выполняем постобработку (mean, median, p95, stddev, range)
        result_df = apply_postprocessing_expressions(result_df, keys_result)
                    
        logger.info(f"Запланирована постобработка по ключам '{keys_aggregate}'")

    # Применяем формулы к данным
    result_df = apply_formulas(result_df, keys_result)

    # Сортировка по keys_sort в Polars
    sort_columns = []
    sort_descending = []
    for key in keys_sort.split(','):
        parts = key.strip().split()
        if len(parts) == 2:
            sort_columns.append(parts[0])
            sort_descending.append(parts[1].lower() != 'asc')  # descending = not ascending
        else:
            sort_columns.append(parts[0])
            sort_descending.append(False)  # По умолчанию ascending

    result_df = result_df.sort(sort_columns, descending=sort_descending)

    logger.info(f"Запланирована сортировка по ключам: {keys_sort}")

    # Ограничение количества строк в соответствии с event_limit
    if event_limit > 0:
        result_df = result_df.head(event_limit)
        logger.info(f"Запланировано ограничение на количество строк: {event_limit}")

    # ВЫПОЛНЯЕМ ВСЕ ПЛАНИРУЕМЫЕ ОПЕРАЦИИ ОДНОВРЕМЕННО
    logger.info("Выполнение всех планируемых операций...")
    
    result_df = result_df.collect()
    del lazy_dfs

    logger.info(f"Выполнены все операции. Результат: {len(result_df)} строк")  
    
    # Восстановление многострочных значений для строковых колонок в Polars
    string_columns = [col for col in result_df.columns if result_df[col].dtype == pl.Utf8]
    if string_columns:
        result_df = result_df.with_columns([
            pl.col(col).str.replace('<_NL_>', '\n') for col in string_columns
        ])

    # Переупорядочиваем колонки в строгом порядке по keys_result
    result_df = reorder_columns_by_keys_result(result_df, keys_result)
    logger.info("Колонки переупорядочены по keys_result")

    # Очищаем временную директорию после выполнения всех операций
    if not keep_temp_files:
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Временная директория {temp_dir} очищена")
        except Exception as e:
            logger.warning(f"Не удалось очистить временную директорию {temp_dir}: {e}")
    else:
        logger.info(f"Промежуточные файлы сохранены в: {temp_dir}")

     # Округляем числовые колонки после collect()
    if keys_aggregate.strip():
        group_keys = [k.strip() for k in keys_aggregate.split(',')]
        result_df = result_df.with_columns([
            pl.col(col).round(2) 
            for col in result_df.columns 
            if col not in group_keys and result_df[col].dtype in [pl.Float64, pl.Float32]
        ])

    # Переименование колонок в alias, если задано name и alias
    result_df = rename_columns_by_keys_result(result_df, keys_result)
    
    # Запись DataFrame в файл Excel (конвертируем Polars в pandas для Excel)
    try:
        # Соединяем директорию и имя файла
        full_output_path = os.path.join(output_directory, output_file)
        # Используем прямой экспорт Polars в Excel
        result_df.write_excel(full_output_path)
        logger.info(f"Запись результата в файл: {full_output_path}")
    except Exception as e:
        logger.error(f"Ошибка при записи в Excel: {e}")
        raise

    end_time = time.time()
    logger.info(f"Общее время выполнения скрипта: {end_time - start_time:.2f} секунд")
    print(f"Общее время выполнения скрипта: {end_time - start_time:.2f} секунд")

    return total_size_mb 