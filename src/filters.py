# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class Filter:
    key: str
    operation: str  # 'eq', 'gt', 'ge', 'lt', 'le', 'contains', 'regex'
    value: Any
    compiled_regex: Optional[re.Pattern] = field(init=False, default=None)

    def __post_init__(self):
        if self.operation == 'regex':
            try:
                self.compiled_regex = re.compile(self.value)
            except re.error as e:
                raise ValueError(f"Неверный регулярный шаблон для фильтра {self.key}: {self.value} - {e}")

    def matches(self, event: Dict[str, Any]) -> bool:
        # Функция для проверки "пустоты" значения
        def is_empty(value: any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return value.strip() == ""
            # Для числовых типов (int, float) считаем, что значение всегда заполнено
            return False

        # Обработка специальных операций "Заполнено" и "Не заполнено"
        if self.operation in ("filled", "empty"):
            # Если ключ отсутствует, считаем его пустым
            if self.key not in event:
                return self.operation == "empty"
            empty = is_empty(event[self.key])
            return (not empty) if self.operation == "filled" else empty

        # Для остальных операций, если ключ отсутствует, возвращаем False
        event_value = event.get(self.key)
        if event_value is None:
            return False

        try:
            if self.operation == 'eq':
                return event_value == self.value
            elif self.operation == 'ne':
                return event_value != self.value
            elif self.operation == 'gt':
                return float(event_value) > float(self.value)
            elif self.operation == 'ge':
                return float(event_value) >= float(self.value)
            elif self.operation == 'lt':
                return float(event_value) < float(self.value)
            elif self.operation == 'le':
                return float(event_value) <= float(self.value)
            elif self.operation == 'contains':
                return self.value in event_value
            elif self.operation == 'regex':
                if not isinstance(event_value, str):
                    return False
                return bool(self.compiled_regex.search(event_value))
            else:
                raise ValueError(f"Unsupported operation: {self.operation}")
        except (ValueError, TypeError):
            return False

class FilterSet:
    def __init__(self, filters: List[Filter]):
        self.filters = filters

    def matches_all(self, event: Dict[str, Any]) -> bool:
        return all(f.matches(event) for f in self.filters)

def load_filters(filter_definitions: List[Dict[str, Any]]) -> FilterSet:
    """
    Создание объекта FilterSet из списка фильтров.

    :param filter_definitions: Список словарей, каждый из которых описывает фильтр.
                               Ожидается, что каждый словарь содержит ключи 'key', 'operation' и 'value'.
    :return: Объект FilterSet, содержащий все фильтры.
    :raises ValueError: Если фильтр имеет недопустимую структуру или содержит неверную операцию.
    """
    filters = []
    for idx, filt in enumerate(filter_definitions, start=1):
        key = filt.get("key")
        operation = filt.get("operation")
        value = filt.get("value")

        if not key or not operation:
            raise ValueError(f"Фильтр под номером {idx} пропущен из-за отсутствия 'key' или 'operation': {filt}")

        try:
            filter_obj = Filter(key=key, operation=operation, value=value)
            filters.append(filter_obj)
        except ValueError as e:
            raise ValueError(f"Ошибка в фильтре под номером {idx}: {e}")

    return FilterSet(filters) 