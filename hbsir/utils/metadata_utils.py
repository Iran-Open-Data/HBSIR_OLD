from typing import Literal, overload


class MetaReader:
    def __init__(
        self,
        metadata: dict,
        year: int,
        year_range: tuple[int, int] = (1350, 1450),
        version_keyword: str = "versions",
        category_keyword: str = "categories",
    ):
        self.metadata = metadata
        self.year = year
        self.year_range = year_range
        self.version_keyword = version_keyword
        self.category_keyword = category_keyword

    def retrieve(self):
        return self._retrive_version(self.metadata)

    @overload
    def _retrive_version(self, element: int) -> int:
        ...

    @overload
    def _retrive_version(self, element: str) -> str:
        ...

    @overload
    def _retrive_version(self, element: list) -> list:
        ...

    @overload
    def _retrive_version(self, element: dict) -> dict:
        ...

    def _retrive_version(self, element):
        if isinstance(element, (int, str)):
            return element
        if isinstance(element, list):
            return [self._retrive_version(value) for value in element]
        if isinstance(element, dict):
            element = self._retrieve_dictionaty_verion(element)
            return {key: self._retrive_version(value) for key, value in element.items()}
        raise TypeError

    def _retrieve_dictionaty_verion(self, dictionaty: dict) -> dict:
        version_type = self._detect_version_type(dictionaty)
        if version_type == "not_versioned":
            return dictionaty
        if version_type == "keyword_versioned":
            versioned_dictionary = dictionaty[self.version_keyword]
        elif version_type == "simple_versioned":
            versioned_dictionary = dictionaty
        else:
            raise ValueError

        latest_verion_number = self._find_verion_number(versioned_dictionary)
        latest_verion: dict = versioned_dictionary[latest_verion_number]

        if version_type == "keyword_versioned":
            latest_verion.update(
                {
                    key: value
                    for key, value in dictionaty.items()
                    if key != self.version_keyword
                }
            )

        return latest_verion

    def _detect_version_type(
        self, dictionaty: dict
    ) -> Literal["keyword_versioned", "simple_versioned", "not_versioned"]:
        if self.version_keyword in dictionaty:
            return "keyword_versioned"
        for element in dictionaty:
            if (
                not isinstance(element, int)
                or (element < self.year_range[0])
                or (element > self.year_range[1])
            ):
                return "not_versioned"
        return "simple_versioned"

    def _find_verion_number(self, dictionaty: dict):
        selected_version = self.year_range[0]
        for version in dictionaty:
            if version <= self.year:
                selected_version = max(selected_version, version)
        return selected_version
