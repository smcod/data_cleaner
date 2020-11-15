import logging
from collections import Counter
from typing import List, Tuple, Dict, Union, Set

import numpy as np
from aiomysql.sa.result import RowProxy

from src.downloader import ITEM_ID_TO_COLUMN

logger = logging.getLogger(__name__)


def get_most_frequent(*args) -> Union[int, None]:
    if len(*args) == 0:
        return None
    if len(*args) == 1:
        return int(args[0].pop())
    counts = Counter(*args)
    most_common_value = counts.most_common(1).pop()
    return int(most_common_value[0])


def get_mean(*args) -> Union[float, None]:
    if len(*args) == 0:
        return None
    if len(*args) == 1:
        return float(args[0].pop())
    return float(np.mean(*args))


def is_integer(num) -> bool:
    return num % 1 == 0.0


def compare_value_with_status_codes(values: list, status_to_define_alert: int = 1 << 13):
    if not values:
        return None
    for cur_val in values:
        if not is_integer(cur_val):
            continue
        if int(cur_val) & status_to_define_alert:
            return 0
    return 1


def _aggregate_data(
        item_to_value: Dict[int, List[Union[int, float]]],
        timestamps: Set[int],
        state_mask: int = 1 << 13,
        convert_state: bool = True
) -> Tuple[int, Dict[int, Union[int, float]]]:

    mean_timestamp = int(np.mean(list(timestamps)))
    output_item_to_value = dict()

    for item_id, column_name in ITEM_ID_TO_COLUMN.items():
        if item_id not in item_to_value:
            output_item_to_value[item_id] = None
            continue

        values = item_to_value[item_id]
        # отдельная обработка для статусов
        if convert_state:
            lower_column_name = column_name.lower()
            is_state = 'state' in lower_column_name or 'status' in lower_column_name
            if is_state:
                output_item_to_value[item_id] = compare_value_with_status_codes(values, state_mask)
                continue

        if not values:
            output_item_to_value[item_id] = None
            continue

        if isinstance(values[0], int):
            new_value = get_most_frequent(values)
        elif isinstance(values[0], float):
            new_value = get_mean(values)
        else:
            logger.error(f'Value not can be: {type(values[0])}')
            continue

        output_item_to_value[item_id] = new_value

    item_to_value.clear()
    timestamps.clear()

    return mean_timestamp, output_item_to_value


def get_transform_values(
        preprocessed_data: List[RowProxy],
        aggregate_interval: int = 20,
        state_mask: int = 1 << 13,
        convert_state: bool = True
) -> Dict[int, Dict[int, Union[int, float]]]:

    item_to_value: Dict[int, List[Union[int, float]]] = dict()
    timestamps: Set[int] = set()
    all_data: Dict[int, Dict[int, Union[int, float]]] = dict()

    if not preprocessed_data:
        return all_data

    start_timestamp = preprocessed_data[0].clock

    for row in preprocessed_data:
        item_id, timestamp, _value = row.itemid, row.clock, row.value

        if start_timestamp + aggregate_interval < timestamp:
            mean_timestamp, output_item_to_value = _aggregate_data(
                item_to_value, timestamps, state_mask=state_mask, convert_state=convert_state
            )
            all_data[mean_timestamp] = output_item_to_value
            start_timestamp += aggregate_interval

        timestamps.add(timestamp)

        if item_id in item_to_value:
            item_to_value[item_id].append(_value)
        else:
            item_to_value[item_id] = [_value]

    return all_data


if __name__ == '__main__':
    from src.etl.zabbix_transform.test_data import TEST_DATA

    out = get_transform_values(TEST_DATA)
    for timestamp, data in out.items():
        print(f"{timestamp}: {data}")