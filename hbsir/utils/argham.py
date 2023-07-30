
from typing import Iterable, overload

class Argham:
    default_start: int | None = None
    default_end: int | None = None
    default_step: int = 1

    def __init__(self, argham: list | dict | int, keywords: list[str] | None = None):
        self.argham = argham
        self.range_list = []
        self.number_list = []
        self.keywords = [] if keywords is None else keywords
        self._parse_argham(argham)

    def _parse_argham(self, argham) -> None:
        if isinstance(argham, list):
            for ragham in argham:
                self._parse_argham(ragham)
        elif isinstance(argham, dict):
            self._parse_dict(argham)
        elif isinstance(argham, int):
            self.number_list.append(argham)
        else:
            pass

    def _parse_dict(self, dictionary: dict) -> None:
        if len(self.keywords) > 0:
            for word in self.keywords:
                if word in dictionary:
                    self._parse_argham(dictionary[word])
        if ("start" in dictionary) or ("end" in dictionary):
            selected_range = self._parse_start_end_dict(dictionary)
            self.range_list.append(selected_range)
        else:
            raise ValueError("Start or end must be specified")

    def _parse_start_end_dict(self, dictionary: dict):
        start = self.default_start
        end = self.default_end
        step = self.default_step

        if "start" in dictionary:
            start = dictionary["start"]
        if "end" in dictionary:
            end = dictionary["end"]

        if start is None:
            raise ValueError("Start must be specified")
        if end is None:
            raise ValueError("End must be specified")

        return range(start, end, step)

    @overload
    def contains(self, values: int) -> bool:
        ...

    @overload
    def contains(self, values: Iterable[int]) -> list[bool]:
        ...

    def contains(self, values: int | Iterable[int]) -> bool | list[bool]:
        if isinstance(values, int):
            return self._contains_int(values)
        result = []
        for value in values:
            result.append(self._contains_int(value))
        return result

    def _contains_int(self, value: int):
        for number_range in self.range_list:
            if value in number_range:
                return True
        if value in self.number_list:
            return True
        return False