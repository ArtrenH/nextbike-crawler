from __future__ import annotations

import dataclasses
import typing

import jsonpath_ng

__all__ = [
    "Preprocessor",
    "JsonDictionarizer",
]


class Preprocessor(typing.Protocol):
    def do(self, data: dict) -> None:
        ...

    def undo(self, data: dict) -> None:
        ...


@dataclasses.dataclass
class JsonDictionarizer:
    rules: list[tuple[str, str]]
    rules_parsed: list[tuple[jsonpath_ng.JSONPath, str]] = dataclasses.field(init=False)

    def __post_init__(self):
        self.rules_parsed = [(jsonpath_ng.parse(jsonpath), key_field) for jsonpath, key_field in self.rules]

    def do(self, data):
        """
        :param rules: A list of (JSONPath, key_field) tuples.
                      For each tuple:
                        - JSONPath is used to find an array in `data`.
                        - key_field is the property name to use as the dict key.
        :param data:  The dictionary to transform in-place.
        :return:      The same dictionary `data` with matched arrays replaced by dicts.
        """
        for jsonpath_expr, key_field in self.rules_parsed:
            matches = jsonpath_expr.find(data)

            for match in matches:
                array_val = match.value

                if isinstance(array_val, list):
                    keyed_dict = {str(item[key_field]): item for item in array_val}

                    match.path.update(match.context.value, keyed_dict)

    def undo(self, data):
        """
        :param rules: A list of (JSONPath, key_field) tuples.
                      For each tuple:
                        - JSONPath indicates where in `data` we expect to find a dict
                          that was previously an array keyed by key_field.
                        - key_field is the property name that was used as the dict key.
        :param data:  The dictionary to transform in-place.
        :return:      The same dictionary `data` with matched dicts replaced by arrays of objects.

        For each match:
          1) We assume the matched value is a dict of shape {key_field_value: item_dict, ...}.
          2) Convert that dict into a list of item_dicts,
             ensuring item_dict[key_field] = key_field_value.
        """

        # data = copy.copy(data)

        for jsonpath_expr, key_field in reversed(self.rules_parsed):
            matches: list[jsonpath_ng.DatumInContext] = jsonpath_expr.find(data)

            for match in matches:
                dict_val = match.value  # .copy()

                array_val = list(dict_val.values())

                # # Replace the dict in the original structure with the new array
                # parent_path = match.path.child(jsonpath_ng.Parent())
                # parent_val = parent_path.find(data)
                # assert len(parent_val) == 1
                #
                # if parent_val[0].path == jsonpath_ng.This():
                #     pass
                # else:
                #     parent_path.update(data, parent_val.context.copy())

                match.path.update(match.context.value, array_val)
