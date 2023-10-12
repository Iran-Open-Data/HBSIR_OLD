"""
Metadata version resolution module.

Provides MetadataVersionResolver class to retrieve specific versions 
of metadata based on given year.

Supports both simple integer and keyword-based versioning of metadata,
configured via MetadataVersionSettings.

Classes
-------
MetadataVersionResolver - Resolves and returns metadata versions.

MetadataVersionSettings - Configurable settings for versioning.

"""
from typing import Literal, overload

from pydantic import BaseModel


class MetadataResolverSettings(BaseModel):
    """
    Settings for metadata versioning.

    Specifies the field names and valid range used for versioning metadata.

    Attributes
    ----------
    year_range : tuple(int, int)
        Valid min and max year for version numbers.
        Default (1350, 1450).

    year_keyword : str
        Metadata field keyword for year. Default "year".

    version_keyword : str
        Metadata field keyword for version section. Default "versions".

    category_keyword : str
        Metadata field keyword for categories. Default "categories".
    """

    year_range: tuple[int, int] = (1350, 1450)
    year_keyword: str = "year"
    version_keyword: str = "versions"
    items_keyword: str = "items"
    category_keyword: str = "categories"
    item_key_name: str = "item_key"


class MetadataVersionResolver:
    """
    Resolves specific metadata versions from versioned metadata.

    This class retrieves the appropriate version of metadata based on a provided
    year and the versioning information contained within the metadata. It supports
    both keyword-based and simple integer versioning schemes for metadata dictionaries.

    The versioning configuration is specified via a MetadataVersionSettings instance.
    The class will recursively traverse the metadata structure to find the right
    version for the given year according to the settings.

    Parameters
    ----------
    metadata : dict
        Metadata to extract version from. This can contain embedded versioning information.

    year : int, optional
        Year to retrieve metadata for. If not provided, the class will try to extract
        the year from the metadata based on the settings.

    settings : MetadataVersionSettings, optional
        Versioning configuration settings. If not provided, default settings will be used.


    Methods
    -------
    get_version() : Retrieves resolved metadata version for given year.
    is_versioned() : Check if given metadata element is versioned.
    """

    def __init__(
        self,
        metadata: dict,
        year: int | None = None,
        settings: MetadataResolverSettings | None = None,
    ):
        self.metadata = metadata
        self.settings = MetadataResolverSettings() if settings is None else settings
        if year is not None:
            self.year = year
        elif (isinstance(metadata, dict)) and (self.settings.year_keyword in metadata):
            self.year = metadata[self.settings.year_keyword]
        elif self.is_versioned():
            raise NameError("Versioned metadata requires year parameter.")

    def get_version(self) -> dict | list | str | int | float | None:
        """Retrieves metadata version for given year.

        Recursively traverses metadata structure and returns appropriate
        version based on configured settings and provided year.

        Returns
        -------
        dict | list | str | int | None
            Resolved metadata version for provided year.

        Examples
        --------
        >>> resolver = MetadataVersionResolver(metadata, year=1380)
        >>> version = resolver.get_version()
        """
        return self._retrive_version(self.metadata)

    @overload
    def _retrive_version(self, element: dict) -> dict | list | str | int | float | None:
        ...

    @overload
    def _retrive_version(self, element: list) -> list:
        ...

    @overload
    def _retrive_version(self, element: str) -> str:
        ...

    @overload
    def _retrive_version(self, element: int) -> int:
        ...

    @overload
    def _retrive_version(self, element: float) -> float:
        ...

    @overload
    def _retrive_version(self, element: None) -> None:
        ...

    def _retrive_version(self, element):
        if (element is None) or isinstance(element, (int, float, str)):
            pass
        elif isinstance(element, list):
            element = [self._retrive_version(value) for value in element]
        elif isinstance(element, dict):
            element = self._retrieve_dictionaty_verion(element)
            if (
                isinstance(element, dict)
                and self._detect_version_type(element) == "not_versioned"
            ):
                element = {
                    key: self._retrive_version(value) for key, value in element.items()
                }
            else:
                element = self._retrive_version(element)
        else:
            raise TypeError(f"Element {element} is not resolved")

        return element

    def is_versioned(self, element: dict | list | str | int | None = None):
        """Check if given metadata element is versioned.

        Recursively checks if the given metadata element contains
        versioning information according to the configuration.

        Parameters
        ----------
        element : dict, list, str, int, None, optional
            Metadata element to check for versioning.
            If not provided, uses the original metadata.

        Returns
        -------
        bool
            True if the element is versioned, False otherwise.

        Raises
        ------
        TypeError
            If element is an unsupported type.

        Examples
        --------
        >>> resolver = MetadataVersionResolver(metadata)
        >>> resolver.is_versioned()
        True
        """
        if element is None:
            element = self.metadata
        if isinstance(element, (int, str)):
            return False
        if isinstance(element, list):
            return not all(not self.is_versioned(value) for value in element)
        if isinstance(element, dict):
            if self._detect_version_type(element) != "not_versioned":
                return True
            return not all(not self.is_versioned(value) for value in element.values())
        raise TypeError

    def _retrieve_dictionaty_verion(
        self, dictionaty: dict
    ) -> dict | list | str | int | None:
        version_type = self._detect_version_type(dictionaty)
        if version_type == "not_versioned":
            return dictionaty
        if version_type == "keyword_versioned":
            versioned_dictionary = dictionaty[self.settings.version_keyword]
        elif version_type == "simple_versioned":
            versioned_dictionary = dictionaty
        else:
            raise ValueError

        latest_verion_number = self._find_verion_number(versioned_dictionary)
        if latest_verion_number == 0:
            latest_verion = {}
        else:
            latest_verion = versioned_dictionary[latest_verion_number]

        if version_type == "keyword_versioned":
            latest_verion = {} if latest_verion is None else latest_verion
            if isinstance(latest_verion, dict):
                latest_verion.update(
                    {
                        key: value
                        for key, value in dictionaty.items()
                        if (key != self.settings.version_keyword)
                        and (key not in latest_verion)
                    }
                )
        latest_verion = None if latest_verion == {} else latest_verion

        return latest_verion

    def _detect_version_type(
        self, dictionaty: dict
    ) -> Literal["keyword_versioned", "simple_versioned", "not_versioned"]:
        if self.settings.version_keyword in dictionaty:
            return "keyword_versioned"
        for element in dictionaty:
            if (
                not isinstance(element, int)
                or (element < self.settings.year_range[0])
                or (element > self.settings.year_range[1])
            ):
                return "not_versioned"
        return "simple_versioned"

    def _find_verion_number(self, dictionaty: dict):
        selected_version = 0
        for version in dictionaty:
            if version <= self.year:
                selected_version = max(selected_version, version)
        return selected_version


class MetadataCategoryResolver(MetadataVersionResolver):
    """Metadata Category Resolver

    This class takes metadata and categorizes it into a list of items with categories.

    It inherits from MetadataVersionResolver to first resolve the appropriate
    version of the metadata based on the provided year.

    Example:
    -------

    Before:

    ```
    metadata:
        key1: val1
        key2: val2

        items:
        item1:
            shared_key_1: shared_val_1
            shared_key_2: shared_val_2

            categories:
            1:
                cat_key_1: cat_val_1
            2:
                cat_key_2: cat_val_2
    ```

    After:
    ```
    metadata:
        key1: val1
        key2: val2

        items:
        - shared_key_1: shared_val_1
            shared_key_2: shared_val_2
            cat1key: catval1
            item_key: item1

        - shared_key_1: shared_val_1
            shared_key_2: shared_val_2
            cat2key: catval2
            item_key: item1
    ```
    """

    # pylint: disable=unsubscriptable-object
    # pylint: disable=unsupported-assignment-operation
    def categorize_metadata(self) -> dict:
        """Categorize metadata dictionary into items list.

        Parses the input metadata to build a categorized list
        of items under the 'items' key.

        Retrieves the latest version of the metadata using get_version().
        Checks that metadata is a dictionary.

        Loops through metadata[items_keyword] accessing each item:
            - Checks if item has 'categories' key, splits into categories
            - Copies over shared keys from item to categories
            - Sets configured key_name in each category
            - Extends item_list with list of categories

        Returns the updated metadata dict with categorized 'items' list.

        Raises
        ------

        TypeError
            If metadata input is not a dictionary.

        Returns
        -------

        dict
            Updated metadata with 'items' list of categorized elements.
        """
        metadata = self.get_version()
        if not isinstance(metadata, dict):
            raise TypeError
        items = []
        for key, item in metadata[self.settings.items_keyword].items():
            item_list = self._get_categories(item)
            for element in item_list:
                element[self.settings.item_key_name] = key.strip("_")
            items.extend(item_list)
        metadata[self.settings.items_keyword] = items
        return metadata

    def _get_categories(self, item: dict) -> list:
        if "categories" not in item:
            categories_list = [item]
        else:
            categories_number = list(item["categories"].keys())
            categories_number.sort()
            categories_list = [
                item["categories"][number] for number in categories_number
            ]
            shared_keys = [key for key in item.keys() if key != "categories"]
            for category in categories_list:
                for key in shared_keys:
                    if key not in category.keys():
                        category[key] = item[key]
        return categories_list
