# Copyright (c) 2025, RDV SOFT, LLC
# SPDX-License-Identifier: BSD-3-Clause

# aggregation_registry.py
import re
import numpy as np
import polars as pl
from typing import Callable, Union, List, Dict

class Aggregation:
    def __init__(self, name: str, postfix: str = '', *,
                 pre_process_expr_fn=None,
                 pre_agg_expr_fn=None,
                 final_agg_expr_fn=None,
                 post_process_expr_fn=None,
                 display_name=None,
                 is_range=False,
                 needs_raw_values=False):
        self.name = name
        self.postfix = postfix
        self.pre_process_expr_fn = pre_process_expr_fn
        self.pre_agg_expr_fn = pre_agg_expr_fn
        self.final_agg_expr_fn = final_agg_expr_fn
        self.post_process_expr_fn = post_process_expr_fn
        self.display_name = display_name or f" ({name})"
        self.is_range = is_range
        self.needs_raw_values = needs_raw_values

class AggregationRegistry:
    def __init__(self):
        self._registry: Dict[str, Aggregation] = {}

    def register(self, agg: Aggregation):
        self._registry[agg.name] = agg

    def get(self, name: str) -> Union[Aggregation, None]:
        return self._registry.get(name)

    def all(self):
        return self._registry

agg_registry = AggregationRegistry()

# Стандартные агрегации
agg_registry.register(Aggregation(
    name='sum', postfix='',
    pre_agg_expr_fn=lambda col: pl.col(col).sum(),
    final_agg_expr_fn=lambda col: pl.col(col).sum(),
    post_process_expr_fn=None,
    display_name=' (sum)'
))

agg_registry.register(Aggregation(
    name='min', postfix='_min',
    pre_agg_expr_fn=lambda col: pl.col(col).min().alias(f"{col}_min"),
    final_agg_expr_fn=lambda col: pl.col(f"{col}_min").min().alias(f"{col}_min"),
    post_process_expr_fn=None,
    display_name=' (min)'
))

agg_registry.register(Aggregation(
    name='max', postfix='_max',
    pre_agg_expr_fn=lambda col: pl.col(col).max().alias(f"{col}_max"),
    final_agg_expr_fn=lambda col: pl.col(f"{col}_max").max().alias(f"{col}_max"),
    post_process_expr_fn=None,
    display_name=' (max)'
))

agg_registry.register(Aggregation(
    name='mean', postfix='_mean',
    pre_agg_expr_fn=lambda col: [
        pl.col(col).sum().alias(f"{col}_mean_sum"),
        pl.col(col).count().alias(f"{col}_mean_count")
    ],
    final_agg_expr_fn=lambda col: [
        pl.col(f"{col}_mean_sum").sum().alias(f"{col}_mean_sum"),
        pl.col(f"{col}_mean_count").sum().alias(f"{col}_mean_count")
    ],
    post_process_expr_fn=lambda col: (
        pl.col(f"{col}_mean_sum") / pl.col(f"{col}_mean_count")
    ).alias(f"{col}_mean"),
    display_name=' (avg)'
))

agg_registry.register(Aggregation(
    name='median', postfix='_median',
    pre_agg_expr_fn=lambda col: pl.col(col).alias(f"{col}_values"),
    final_agg_expr_fn=lambda col: pl.col(f"{col}_values").flatten().alias(f"{col}_values"),
    post_process_expr_fn=lambda col: pl.col(f"{col}_values").map_elements(
        lambda arr: float(np.median(arr.to_list()))
        if hasattr(arr, "to_list") and arr is not None and len(arr) > 0 else None,
        return_dtype=pl.Float64
    ).alias(f"{col}_median"),
    display_name=' (mdn)',
    needs_raw_values=True
))

agg_registry.register(Aggregation(
    name='p95', postfix='_p95',
    pre_agg_expr_fn=lambda col: pl.col(col).alias(f"{col}_values"),
    final_agg_expr_fn=lambda col: pl.col(f"{col}_values").flatten().alias(f"{col}_values"),
    post_process_expr_fn=lambda col: pl.col(f"{col}_values").map_elements(
        lambda arr: float(np.percentile(arr.to_list(), 95))
        if hasattr(arr, "to_list") and arr is not None and len(arr) > 0 else None,
        return_dtype=pl.Float64
    ).alias(f"{col}_p95"),
    display_name=' (P95)',
    needs_raw_values=True
))

agg_registry.register(Aggregation(
    name='stddev', postfix='_stddev',
    pre_agg_expr_fn=lambda col: pl.col(col).alias(f"{col}_values"),
    final_agg_expr_fn=lambda col: pl.col(f"{col}_values").flatten().alias(f"{col}_values"),
    post_process_expr_fn=lambda col: pl.col(f"{col}_values").map_elements(
        lambda arr: float(np.std(arr.to_list()))
        if hasattr(arr, "to_list") and arr is not None and len(arr) > 0 else None,
        return_dtype=pl.Float64
    ).alias(f"{col}_stddev"),
    display_name=' (stddev)',
    needs_raw_values=True
))

agg_registry.register(Aggregation(
    name='percent', postfix='_percent',
    pre_agg_expr_fn=None,
    final_agg_expr_fn=None,
    post_process_expr_fn=lambda col: (
        (pl.col(col) / pl.col(col).sum() * 100).round(2).alias(f"{col}_percent")
    ),
    display_name=' (% влияния)'
))

# Регистрация intervals-агрегаций по паттерну intervals_X_Y
def register_intervals(step: int, count: int):
    name = f"intervals_{step}_{count}"

    def pre_agg_expr_fn(col):
        exprs = []
        for i in range(count):
            lower = step * i
            upper = step * (i + 1)
            if i == 0:
                label = f"{col}_{name}_<{upper}_count"
            else:
                label = f"{col}_{name}_{lower}-{upper}_count"
            exprs.append(
                pl.col(col).map_elements(
                    lambda arr, low=lower, up=upper: sum(low <= x < up for x in arr) if arr is not None else 0,
                    return_dtype=pl.Int32
                ).alias(label)
            )
        threshold = step * count
        exprs.append(
            pl.col(col).map_elements(lambda arr: sum(x >= threshold for x in arr) if arr is not None else 0, return_dtype=pl.Int32).alias(f"{col}_{name}_{threshold}>_count")
        )
        exprs.append(
            pl.col(col).map_elements(lambda arr: len(arr) if arr is not None else 0, return_dtype=pl.Int32).alias(f"{col}_{name}_total_count")
        )
        return exprs

    def final_agg_expr_fn(col):
        exprs = []
        for i in range(count):
            lower = step * i
            upper = step * (i + 1)
            if i == 0:
                label = f"{col}_{name}_<{upper}_count"
            else:
                label = f"{col}_{name}_{lower}-{upper}_count"
            exprs.append(pl.col(label).sum().alias(label))
        exprs.append(pl.col(f"{col}_{name}_{step * count}>_count").sum().alias(f"{col}_{name}_{step * count}>_count"))
        exprs.append(pl.col(f"{col}_{name}_total_count").sum().alias(f"{col}_{name}_total_count"))
        return exprs

    def post_process_expr_fn(col):
        exprs = []
        total_label = f"{col}_{name}_total_count"
        for i in range(count):
            lower = step * i
            upper = step * (i + 1)
            if i == 0:
                count_label = f"{col}_{name}_<{upper}_count"
            else:
                count_label = f"{col}_{name}_{lower}-{upper}_count"
            percent_label = count_label.replace('_count', '')
            exprs.append((pl.col(count_label) / pl.col(total_label) * 100).round(2).alias(percent_label))
        count_label = f"{col}_{name}_{step * count}>_count"
        percent_label = count_label.replace('_count', '')
        exprs.append((pl.col(count_label) / pl.col(total_label) * 100).round(2).alias(percent_label))
        return exprs

    agg_registry.register(Aggregation(
        name=name,
        postfix='',
        pre_agg_expr_fn=pre_agg_expr_fn,
        final_agg_expr_fn=final_agg_expr_fn,
        post_process_expr_fn=post_process_expr_fn,
        display_name=f" (intervals {step}*{count})",
        is_range=True
    ))

def register_all_intervals_from_config(keys_result: List[Dict]):
    seen_intervals = set()

    for key in keys_result:
        for agg in key.get("aggregation", []):
            match = re.fullmatch(r"intervals_(\d+)_(\d+)", agg)
            if match:
                step, count = int(match[1]), int(match[2])
                if (step, count) not in seen_intervals:
                    register_intervals(step, count)
                    seen_intervals.add((step, count))

def register_time_rounding_aggregation():
    pattern = re.compile(r'^roundtime_(\d+)([smhd])$')

    def pre_process_expr_fn_factory(name: str):
        match = pattern.match(name)
        if not match:
            raise ValueError(f"Некорректный формат агрегата: {name}")
        value, unit = int(match.group(1)), match.group(2)
        offset_str = f"{value}{unit}"

        def expr_fn(col_name):
            return pl.col(col_name).dt.truncate(offset_str).alias(col_name)

        return expr_fn

    def register_round(name: str):
        match = pattern.match(name)
        if not match:
            raise ValueError(f"Некорректный формат агрегата: {name}")
        suffix = f"{match.group(1)}{match.group(2)}"  # только часть после "roundtime_"
        agg_registry.register(Aggregation(
            name=name,
            pre_process_expr_fn=pre_process_expr_fn_factory(name),
            display_name=f" (round {suffix})"
        ))

    # Допускаем любые значения от 1 до 9999 в любом интервале
    for unit in ['s', 'm', 'h', 'd']:
        for val in range(1, 10000):
            register_round(f"roundtime_{val}{unit}")

register_time_rounding_aggregation()
