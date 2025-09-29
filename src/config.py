# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

"""
Модуль для работы с конфигурацией приложения.

Этот модуль отвечает за загрузку и валидацию конфигурационного файла.
Конфигурационный файл (config.json) всегда находится в том же каталоге,
что и исполняемый файл (exe или main.py).
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class ConfigError(Exception):
    """Базовый класс для ошибок конфигурации."""
    pass

class ConfigFileNotFoundError(ConfigError):
    """Возникает, когда конфигурационный файл не найден."""
    pass

class ConfigValidationError(ConfigError):
    """Возникает при ошибках валидации конфигурации."""
    pass

class ConfigParser:
    """
    Класс для работы с конфигурационным файлом.
    
    Attributes:
        REQUIRED_KEYS (List[str]): Список обязательных ключей конфигурации
    """
    
    # Обязательные параметры конфигурации и их назначение:
    REQUIRED_KEYS = [
        "logger_directory",   # Папка, в которую будут сохраняться логи приложения
        "logger_file",       # Имя файла для основного лога приложения
        "base_directories",  # Список базовых директорий, где искать логи 1С
        "base_subdirectories", # Список поддиректорий, которые нужно анализировать
        "output_directory",  # Папка для сохранения итогового Excel-файла
        "output_file",       # Имя итогового Excel-файла с результатами парсинга
        "start_period",      # Начало периода для фильтрации логов (строка, формат YYMMDDhhmmss)
        "end_period",        # Конец периода для фильтрации логов (строка, формат YYMMDDhhmmss)
        "event_name",        # Имя события, которое анализируется
        "event_limit",       # Максимальное количество событий для обработки
        "keys_result",       # Список ключей (столбцов) для итогового отчёта
        "keys_aggregate",    # Ключи для агрегации данных (например, группировка)
        "keys_sort",         # Ключи для сортировки итогового отчёта
        "cpu_usage_percent"  # Процент использования CPU для многопроцессорной обработки
    ]

    @staticmethod
    def get_application_path() -> Path:
        """
        Получает путь к каталогу приложения (exe-файла или скрипта).

        Returns:
            Path: Путь к каталогу приложения

        Note:
            Для exe-файла возвращает родительский каталог исполняемого файла.
            Для Python-скрипта возвращает каталог текущего файла.
        """
        if getattr(sys, 'frozen', False):
            return Path(sys.executable).parent
        return Path(__file__).parent

    @staticmethod
    def get_config_path(config_filename: str = 'config.json') -> Path:
        """
        Получает путь к конфигурационному файлу.

        Args:
            config_filename: Имя конфигурационного файла

        Returns:
            Path: Путь к конфигурационному файлу

        Raises:
            ConfigFileNotFoundError: Если файл не найден
        """
        config_path = ConfigParser.get_application_path() / config_filename
        if not config_path.exists():
            error_msg = f"Конфигурационный файл {config_filename} не найден в каталоге {config_path.parent}"
            logger.error(error_msg)
            raise ConfigFileNotFoundError(error_msg)
        return config_path

    @staticmethod
    def detect_encoding(file_path: Path) -> str:
        """
        Определяет кодировку файла.

        Args:
            file_path: Путь к файлу

        Returns:
            str: Определенная кодировка или 'utf-8' по умолчанию
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

    @staticmethod
    def load_config(config_filename: str = 'config.json') -> Dict[str, Any]:
        """
        Загружает и парсит конфигурационный файл.

        Args:
            config_filename: Имя конфигурационного файла

        Returns:
            Dict[str, Any]: Загруженная конфигурация

        Raises:
            ConfigFileNotFoundError: Если файл не найден
            ConfigError: При ошибках парсинга JSON
        """
        try:
            config_path = ConfigParser.get_config_path(config_filename)
            encoding = ConfigParser.detect_encoding(config_path)
            
            with open(config_path, 'r', encoding=encoding) as config_file:
                return json.load(config_file)
        except json.JSONDecodeError as e:
            error_msg = f"Ошибка при разборе JSON-конфигурации: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg) from e

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Проверяет наличие всех необходимых параметров в конфигурации.

        Args:
            config: Словарь с конфигурацией

        Raises:
            ConfigValidationError: Если отсутствуют обязательные параметры
        """
        missing_keys = [key for key in ConfigParser.REQUIRED_KEYS if key not in config]
        if missing_keys:
            error_msg = f"Отсутствуют необходимые параметры в конфигурации: {', '.join(missing_keys)}"
            logger.error(error_msg)
            raise ConfigValidationError(error_msg) 