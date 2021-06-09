from typing import List, Any


def get_unique_value(target_list: List[Any], unique_fields: List[str], value: Any) -> [Any, None]:
    for exist_value in target_list:
        if all(exist_value[f] == value[f] for f in unique_fields):
            return exist_value


def add_unique_to_list(target_list: List[Any], unique_fields: List[str], value: Any) -> int:
    for idx, exist_value in enumerate(target_list):
        if all(exist_value[f] == value[f] for f in unique_fields):
            return idx
    target_list.append(value)
    return len(target_list) - 1
