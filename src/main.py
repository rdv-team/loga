# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

"""
Парсер технологического журнала 1С Предприятия.

Разработчик: RDV
Контакты: www.rdv-it.ru
Версия: 1.0.0

Этот модуль является точкой входа для приложения парсинга.
Он координирует загрузку конфигурации, настройку логирования и процесс парсинга.

Пример использования:
    $ python main.py

Скрипт ожидает наличия файла config.json в той же директории с необходимой конфигурацией.
"""

from typing import Optional, NoReturn
from logging import Logger
from multiprocessing import freeze_support

from logger import setup_basic_logger, setup_main_logger
from config import ConfigParser
from data_processing import parse_logs_to_xlsx

class ParserError(Exception):
    """Базовый класс исключений для ошибок парсера."""
    pass


class ConfigurationError(ParserError):
    """Возникает при проблемах с загрузкой или валидацией конфигурации."""
    pass


class LoggerSetupError(ParserError):
    """Возникает при проблемах с инициализацией логгера."""
    pass


def setup_configuration(logger: Logger) -> dict:
    """
    Загружает и проверяет конфигурацию приложения.

    Аргументы:
        logger: Экземпляр логгера для отчетов об ошибках

    Возвращает:
        dict: Проверенный словарь конфигурации

    Исключения:
        ConfigurationError: Если загрузка или проверка конфигурации не удалась
    """
    try:
        config = ConfigParser.load_config()
        ConfigParser.validate_config(config)
        return config
    except Exception as e:
        error_msg = f"Ошибка конфигурации: {str(e)}"
        logger.error(error_msg)
        raise ConfigurationError(error_msg) from e


def initialize_main_logger(config: dict, basic_logger: Logger) -> Logger:
    """
    Инициализирует основной логгер приложения.

    Аргументы:
        config: Словарь конфигурации приложения
        basic_logger: Экземпляр базового логгера для отчетов об ошибках

    Возвращает:
        Logger: Настроенный экземпляр основного логгера

    Исключения:
        LoggerSetupError: Если настройка логгера не удалась
    """
    try:
        return setup_main_logger(config)
    except Exception as e:
        error_msg = f"Ошибка настройки основного логгера: {str(e)}"
        basic_logger.error(error_msg)
        raise LoggerSetupError(error_msg) from e


def process_logs(config: dict, logger: Logger) -> float:
    """
    Обрабатывает логи согласно конфигурации.

    Аргументы:
        config: Словарь конфигурации приложения
        logger: Экземпляр логгера для отчетов о состоянии

    Возвращает:
        float: Общий размер обработанных логов в МБ

    Исключения:
        ParserError: Если обработка логов не удалась
    """
    try:
        total_size_mb = parse_logs_to_xlsx(config)
        logger.info(f"Общий размер обработанных логов: {total_size_mb:.2f} МБ")
        logger.info(f"Парсинг успешно завершен. Результаты сохранены в: {config['output_file']}")
        return total_size_mb
    except Exception as e:
        error_msg = f"Ошибка парсинга логов: {str(e)}"
        logger.error(error_msg)
        raise ParserError(error_msg) from e


def main() -> Optional[float]:
    """
    Основная функция для оркестрации процесса парсинга.

    Возвращает:
        Optional[float]: Общий размер обработанных логов в МБ, или None если обработка не удалась
    """
    basic_logger = setup_basic_logger()

    try:
        config = setup_configuration(basic_logger)
        logger = initialize_main_logger(config, basic_logger)
        return process_logs(config, logger)
    except ParserError as e:
        print(f"Ошибка: {str(e)}")
        return None


def run() -> NoReturn:
    """
    Точка входа в приложение с поддержкой многопроцессорной обработки.
    """
    freeze_support()
    main()

if __name__ == "__main__":
    run() 