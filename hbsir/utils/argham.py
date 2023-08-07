from typing import Iterable


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

    def __init__(
        self,
        argham: list | dict | int,
        keywords: list[str] | None = None,
        default_start: int | None = None,
        default_end: int | None = None,
        default_step: int = 1,
    ):
        self.argham = argham
        self.range_list = []
        self.number_list = []
        self.keywords = [] if keywords is None else keywords
        self.default_start = default_start
        self.default_end = default_end
        self.default_step = default_step
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

    def __repr__(self) -> str:
        representation_list = []
        if len(self.number_list) > 0:
            str_numbers = [str(number) for number in self.number_list]
            representation_list.append(f"[{', '.join(str_numbers)}]")
        if len(self.range_list) > 0:
            for rng in self.range_list:
                representation_list.append(f"({min(rng)} - {max(rng)})")
        return ", ".join(representation_list)

    def __contains__(self, value: int):
        if self.min is None:
            return False
        if (value < self.min) or (value > self.max):  # type: ignore
            return False
        for number_range in self.range_list:
            if value in number_range:
                return True
        if value in self.number_list:
            return True
        return False

    def _parse_argham(self, argham) -> None:
        if isinstance(argham, list):
            for ragham in argham:
                self._parse_argham(ragham)
        elif isinstance(argham, dict):
            self._parse_dict(argham)
        elif isinstance(argham, int):
            self.number_list.append(argham)
            self._update_min(argham)
            self._update_max(argham)
        else:
            pass

    def _parse_dict(self, dictionary: dict) -> None:
        if len(self.keywords) > 0:
            for word in self.keywords:
                if word in dictionary:
                    self._parse_argham(dictionary[word])
                    return
        if ("start" in dictionary) or ("end" in dictionary):
            self._parse_start_end_dict(dictionary)
        else:
            raise ValueError("Start or end must be specified")

    def _parse_start_end_dict(self, dictionary: dict) -> None:
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

        self.range_list.append(range(start, end, step))
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
