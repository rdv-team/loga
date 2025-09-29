# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

import logging
import os
from pathlib import Path
import sys

def get_application_path() -> Path:
    """
    Получает путь к каталогу приложения (exe файла или скрипта).
    """
    if getattr(sys, 'frozen', False):
        # Если это скомпилированное приложение
        return Path(sys.executable).parent
    else:
        # Если это обычный Python скрипт
        return Path(__file__).parent

def get_log_file_path(config: dict) -> str:
    """
    Формирует полный путь к файлу лога из параметров конфигурации.
    """
    return os.path.join(config["logger_directory"], config["logger_file"])


def setup_basic_logger() -> logging.Logger:
    """
    Настраивает базовый логгер для записи логов в default.log и консоль.
    """
    logger = logging.getLogger("basic_logger")
    logger.setLevel(logging.INFO)

    # Проверка, если хэндлер уже добавлен
    if not logger.handlers:
        # Лог-файл в каталоге приложения
        log_file = get_application_path() / "parse_1c_logs.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Консольный хэндлер
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger

def setup_main_logger(config: dict) -> logging.Logger:
    """
    Настраивает основной логгер для записи логов в указанный файл и консоль.
    Настраивает корневой логгер, чтобы все модули писали в один файл.
    """
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Очищаем существующие хэндлеры
    root_logger.handlers.clear()

    # Получаем путь к файлу лога
    log_file = Path(get_log_file_path(config))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Создаем файловый хэндлер
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(name)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Добавляем хэндлеры к корневому логгеру
    root_logger.addHandler(file_handler)
    
    # Консольный хэндлер (всегда включен)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Возвращаем логгер для текущего модуля
    return logging.getLogger(__name__)