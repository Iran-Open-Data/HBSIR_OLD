"""
Tests for metadata functions
"""

from hbsir.metadata import (
    metadatas,
    get_latest_version_year as get_last_ver,
    get_metadata_version as get_meta_ver,
    get_categories as get_cat,
)

meta_ins = metadatas.instruction

simple_metadata = {"key": 1380, "other_key": "other_value"}

simple_versioned_metadata = {
    1363: {"key": 1363},
    1383: {"key": 1383},
}

keyword_versioned_metadata = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
    "versions": {
        1363: {"key": 1363},
        1383: {"key": 1383, "overwritten_key": "new_value"},
    },
}


categorized_metadata = {
    "shared_key": "shared_value",
    "overwritten_key": "old_value",
    "categories": {
        1: {"key": "first_value"},
        2: {"key": "second_value", "overwritten_key": "new_value"},
    },
}


class TestWithLocalMetadata:
    "This class test function with above dictionaries"

    def test_get_latest_version_year(self):
        """Test get_latest_version_year function"""
        # Test simple versioned metadata
        assert get_last_ver(simple_versioned_metadata, 1363) == 1363
        assert get_last_ver(simple_versioned_metadata, 1370) == 1363
        assert get_last_ver(simple_versioned_metadata, 1383) == 1383
        assert get_last_ver(simple_versioned_metadata, 1390) == 1383

        # Test key-word versioned metadata
        assert get_last_ver(keyword_versioned_metadata, 1363) is True
        assert get_last_ver(keyword_versioned_metadata, 1370) is True
        assert get_last_ver(keyword_versioned_metadata, 1383) is True
        assert get_last_ver(keyword_versioned_metadata, 1390) is True

        # Test non-versioned metadata
        assert get_last_ver(simple_metadata, 1360) is False
        assert get_last_ver(simple_metadata, 1383) is False
        assert get_last_ver(simple_metadata, 1390) is False

    def test_get_metadata_version(self):
        """Test get_metadata_version function"""
        # Test simple versioned metadata
        assert get_meta_ver(simple_versioned_metadata, 1363)["key"] == 1363
        assert get_meta_ver(simple_versioned_metadata, 1370)["key"] == 1363
        assert get_meta_ver(simple_versioned_metadata, 1390)["key"] == 1383

        # Test key-word versioned metadata
        assert get_meta_ver(keyword_versioned_metadata, 1363)["key"] == 1363
        assert get_meta_ver(keyword_versioned_metadata, 1370)["key"] == 1363
        assert get_meta_ver(keyword_versioned_metadata, 1390)["key"] == 1383

        for i in range(1363, 1401):
            assert (
                get_meta_ver(keyword_versioned_metadata, i)["shared_key"]
                == "shared_value"
            )

        assert (
            get_meta_ver(keyword_versioned_metadata, 1363)["overwritten_key"]
            == "old_value"
        )
        assert (
            get_meta_ver(keyword_versioned_metadata, 1370)["overwritten_key"]
            == "old_value"
        )
        assert (
            get_meta_ver(keyword_versioned_metadata, 1390)["overwritten_key"]
            == "new_value"
        )

        # Test non-versioned metadata
        assert get_meta_ver(simple_metadata, 1360) == simple_metadata
        assert get_meta_ver(simple_metadata, 1383) == simple_metadata
        assert get_meta_ver(simple_metadata, 1390) == simple_metadata

    def test_get_categories(self):
        """Test get_categories function"""
        # Test valid categorized metadata
        assert get_cat(categorized_metadata)[0]["shared_key"] == "shared_value"
        assert get_cat(categorized_metadata)[1]["shared_key"] == "shared_value"

        assert get_cat(categorized_metadata)[0]["key"] == "first_value"
        assert get_cat(categorized_metadata)[1]["key"] == "second_value"

        assert get_cat(categorized_metadata)[0]["overwritten_key"] == "old_value"
        assert get_cat(categorized_metadata)[1]["overwritten_key"] == "new_value"

        # Test non-categorized metadata
        assert get_cat(simple_metadata) == [simple_metadata]


class TestWithInstruction:
    """Test with _instruction.yaml"""

    def test_simple_versioning(self):
        """Test Simple Versioning"""
        for year in range(1360, 1380):
            assert (
                get_meta_ver(meta_ins["simple_versioned"], year)
                == meta_ins["simple_versioned_1360-1379"]
            )
        for year in range(1380, 1400):
            assert (
                get_meta_ver(meta_ins["simple_versioned"], year)
                == meta_ins["simple_versioned_1380"]
            )

    def test_keyword_versioning(self):
        """Test Keyword Versioning"""
        for year in range(1360, 1380):
            assert (
                get_meta_ver(meta_ins["keyword_versioned"], year)
                == meta_ins["keyword_versioned_1360-1379"]
            )
        for year in range(1380, 1400):
            assert (
                get_meta_ver(meta_ins["keyword_versioned"], year)
                == meta_ins["keyword_versioned_1380"]
            )

    def test_categorizing(self):
        """Test Categorizing"""
        assert get_cat(meta_ins["categorized"]) == meta_ins["categorized_opened"]
