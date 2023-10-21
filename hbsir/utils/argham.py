from typing import Iterable

from pydantic import BaseModel


class ArghamDefaults(BaseModel):
    keywords: list[str] = []
    default_start: int | None = None
    default_end: int | None = None
    default_step: int = 1
    default_range: tuple[int, int] | None = None


class Argham:
    """Flexible argument handler for numbers and ranges.

    Accepts numbers, ranges, lists, and dicts to define a set
    of numbers and numeric ranges. Parses the input into
    number_list and range_list.

    Implements membership testing via 'in' operator based
    on the contained numbers and ranges.

    Can be used to flexibly defined sets of numbers/ranges
    and perform set-like membership tests on them.

    Parameters
    ----------
    argham : list, dict, int
        Input content. Can mix numbers, ranges, lists, and dicts.

    keywords : list of str, optional
        Keys in dict argham to parse as ranges.

    default_start : int, optional
        Default start for ranges when not specified.

    default_end : int, optional
        Default end for ranges when not specified.

    Attributes
    ----------
    number_list : list of int
        Parsed individual number values.

    range_list : list of range
        Parsed numeric ranges.

    Methods
    -------
    __contains__(value)
        Implements 'in' membership check.

    check_contained(values)
        Check membership for single or multiple values.

    Examples
    --------
    >>> arg = Argham([1, 2, 3, 4, {'start':6, 'end':10}")
    >>> 2 in arg
    True

    >>> arg.check_contained([4, 5, 6])
    [True, False, True]
    """

    def __init__(self, argham: list | dict | int | None = None, **kwargs):
        self.range_set = set()
        self.defaults = ArghamDefaults(**kwargs)
        self.min: int | None = None
        self.max: int | None = None
        self._parse_argham(argham)

    def check_contained(self, values: int | Iterable[int]) -> bool | list[bool]:
        """Check membership of values in container.

        For a single value, checks whether the value is a member
        of the container using `in` and returns a bool result.

        For multiple values given as an iterable, checks each value
        individually and returns a list of boolean results.

        Parameters
        ----------
        values: int or iterable of int
            Single value or iterable of values to check

        Returns
        -------
        bool or list of bool
            If single value, bool indicating membership.
            If multiple values, list of bool indicating membership
            for each value.

        Examples
        --------
        >>> container.check_membership(2)
        True

        >>> container.check_membership([1, 2, 3])
        [False, True, False]
        """
        if isinstance(values, int):
            return values in self
        result = []
        for value in values:
            result.append(value in self)
        return result

    def get_numbers(self) -> set[int]:
        numbers = set()
        for rng in self.range_set:
            numbers = numbers.union(set(rng))
        return numbers

    def __repr__(self) -> str:
        integers = []
        ranges = []

        for rng in self.range_set:
            if rng.stop - rng.start == 1:
                integers.append(str(rng.start))
            else:
                ranges.append((rng.start, rng.stop))
        representation_list = []
        if len(integers) > 0:
            representation_list.append(f"[{', '.join(integers)}]")
        for rng in ranges:
            representation_list.append(f"({rng[0]} - {rng[1]})")
        return ", ".join(representation_list)

    def __contains__(self, value: int):
        if self.min is None:
            return False
        if (value < self.min) or (value > self.max):  # type: ignore
            return False
        for number_range in self.range_set:
            if value in number_range:
                return True
        return False

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, int):
            if (len(self.range_set) == 1) and (__value in self.range_set.pop()):
                return True
        if isinstance(__value, range):
            if (len(self.range_set) == 1) and (self.range_set.pop() == __value):
                return True
        if isinstance(__value, Argham):
            if self.range_set == __value.range_set:
                return True
            if self.get_numbers() == __value.get_numbers():
                return True
        return False

    def __add__(self, __other: "Argham") -> "Argham":
        if self.defaults != __other.defaults:
            print(f"Warning! different defaults! {self.defaults}, {__other.defaults}")
        result = Argham()
        result.defaults = self.defaults
        result.range_set = self.range_set.union(__other.range_set)

        if (self.min is None) and (__other.min is None):
            result.min = None
        elif self.min is None:
            result.min = __other.min
        elif __other.min is None:
            result.min = self.min
        else:
            result.min = min(self.min, __other.min)

        if (self.max is None) and (__other.max is None):
            result.max = None
        elif self.max is None:
            result.max = __other.max
        elif __other.max is None:
            result.max = self.max
        else:
            result.max = max(self.max, __other.max)
        return result

    def _parse_argham(self, argham) -> None:
        if isinstance(argham, list):
            for ragham in argham:
                self._parse_argham(ragham)
        elif isinstance(argham, dict):
            self._parse_dict(argham)
        elif isinstance(argham, int):
            drng = self.defaults.default_range
            if (drng is not None) and ((argham < drng[0]) or (argham > drng[1])):
                return
            self.range_set.add(range(argham, argham + 1))
            self._update_min(argham)
            self._update_max(argham)
        else:
            pass

    def _parse_dict(self, dictionary: dict) -> None:
        if len(self.defaults.keywords) > 0:
            for word in self.defaults.keywords:
                if word in dictionary:
                    self._parse_argham(dictionary[word])
                    return
        if ("start" in dictionary) or ("end" in dictionary):
            self._parse_start_end_dict(dictionary)
        else:
            for value in dictionary.values():
                self._parse_argham(value)

    def _parse_start_end_dict(self, dictionary: dict) -> None:
        start = self.defaults.default_start
        end = self.defaults.default_end
        step = self.defaults.default_step

        if "start" in dictionary:
            start = dictionary["start"]
        if "end" in dictionary:
            end = dictionary["end"]
        if "step" in dictionary:
            step = dictionary["step"]

        if start is None:
            raise ValueError("Start must be specified")
        if end is None:
            raise ValueError("End must be specified")

        self.range_set.add(range(start, end, step))
        self._update_min(start)
        self._update_max(end - 1)

    def _update_min(self, number) -> None:
        if self.min is None:
            self.min = number
        elif number < self.min:
            self.min = number

    def _update_max(self, number) -> None:
        if self.max is None:
            self.max = number
        elif self.max < number:
            self.max = number
