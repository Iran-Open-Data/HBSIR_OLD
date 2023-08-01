from typing import Literal, overload


class MetaReader:
    def __init__(
        self,
        metadata: dict,
        year: int | None = None,
        year_range: tuple[int, int] = (1350, 1450),
        year_keyword: str = "year",
        version_keyword: str = "versions",
        category_keyword: str = "categories",
    ):
        self.metadata = metadata
        self.year_range = year_range
        self.version_keyword = version_keyword
        self.category_keyword = category_keyword
        if year is not None:
            self.year = year
        elif year_keyword in metadata:
            self.year = metadata[year_keyword]
        elif self._is_versioned(metadata):
            raise NameError("Versioned metadata requires year parameter.")

    def retrieve(self) -> dict | list | str | int | None:
        return self._retrive_version(self.metadata)

    @overload
    def _retrive_version(self, element: None) -> None:
        ...

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
    def _retrive_version(self, element: dict) -> dict | list | str | int | None:
        ...

    def _retrive_version(self, element):
        if element is None:
            return None
        if isinstance(element, (int, str)):
            return element
        if isinstance(element, list):
            return [self._retrive_version(value) for value in element]
        if isinstance(element, dict):
            element = self._retrieve_dictionaty_verion(element)
            if isinstance(element, dict):
                return {key: self._retrive_version(value) for key, value in element.items()}
            return self._retrive_version(element)
        raise TypeError

    def _is_versioned(self, element):
        if isinstance(element, (int, str)):
            return False
        if isinstance(element, list):
            return not all(not self._is_versioned(value) for value in element)
        if isinstance(element, dict):
            if self._detect_version_type(element) != "not_versioned":
                return True
            return not all(not self._is_versioned(value) for value in element.values())
        raise TypeError

    def _retrieve_dictionaty_verion(self, dictionaty: dict) -> dict | list | str | int | None:
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
        if latest_verion_number == 0:
            latest_verion = {}
        else:
            latest_verion = versioned_dictionary[latest_verion_number]

        if (version_type == "keyword_versioned") and isinstance(latest_verion, dict):
            latest_verion.update(
                {
                    key: value
                    for key, value in dictionaty.items()
                    if (key != self.version_keyword) and (key not in latest_verion)
                }
            )
        latest_verion = None if latest_verion == {} else latest_verion

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
        selected_version = 0
        for version in dictionaty:
            if version <= self.year:
                selected_version = max(selected_version, version)
        return selected_version
