"""
Tests for metadata functions
"""

from hbsir.metadata import (
    get_latest_version_year,
    get_metadata_version,
    get_categories,
)


simple_metadata = {
    'key': 1380,
    'other_key': 'other_value'
}

simple_versioned_metadata = {
    1363: {'key': 1363},
    1383: {'key': 1383},
}

key_word_versioned_metadata = {
    'shared_key': 'shared_value',
    'overwritten_key': 'old_value',
    'versions': {
        1363: {'key': 1363},
        1383: {
            'key': 1383,
            'overwritten_key': 'new_value'
        },
    }
}


categorized_metadata = {
    'shared_key': 'shared_value',
    'overwritten_key': 'old_value',
    'categories': {
        1: {'key': 'first_value'},
        2: {
            'key': 'second_value',
            'overwritten_key': 'new_value'
        },
    }
}


def test_get_latest_version_year():
    """Test get_latest_version_year function"""
    # Test simple versioned metadata
    assert get_latest_version_year(simple_versioned_metadata, 1363) == 1363
    assert get_latest_version_year(simple_versioned_metadata, 1370) == 1363
    assert get_latest_version_year(simple_versioned_metadata, 1383) == 1383
    assert get_latest_version_year(simple_versioned_metadata, 1390) == 1383

    # Test key-word versioned metadata
    assert get_latest_version_year(key_word_versioned_metadata, 1363) is True
    assert get_latest_version_year(key_word_versioned_metadata, 1370) is True
    assert get_latest_version_year(key_word_versioned_metadata, 1383) is True
    assert get_latest_version_year(key_word_versioned_metadata, 1390) is True

    # Test non-versioned metadata
    assert get_latest_version_year(simple_metadata, 1360) is False
    assert get_latest_version_year(simple_metadata, 1383) is False
    assert get_latest_version_year(simple_metadata, 1390) is False


def test_get_metadata_version():
    """Test get_metadata_version function"""
    # Test simple versioned metadata
    assert get_metadata_version(simple_versioned_metadata, 1363)['key'] == 1363
    assert get_metadata_version(simple_versioned_metadata, 1370)['key'] == 1363
    assert get_metadata_version(simple_versioned_metadata, 1390)['key'] == 1383

    # Test key-word versioned metadata
    assert get_metadata_version(key_word_versioned_metadata, 1363)['key'] == 1363
    assert get_metadata_version(key_word_versioned_metadata, 1370)['key'] == 1363
    assert get_metadata_version(key_word_versioned_metadata, 1390)['key'] == 1383

    assert get_metadata_version(key_word_versioned_metadata, 1363)['shared_key'] == 'shared_value'
    assert get_metadata_version(key_word_versioned_metadata, 1370)['shared_key'] == 'shared_value'
    assert get_metadata_version(key_word_versioned_metadata, 1390)['shared_key'] == 'shared_value'

    assert get_metadata_version(key_word_versioned_metadata, 1363)['overwritten_key'] == 'old_value'
    assert get_metadata_version(key_word_versioned_metadata, 1370)['overwritten_key'] == 'old_value'
    assert get_metadata_version(key_word_versioned_metadata, 1390)['overwritten_key'] == 'new_value'

    # Test non-versioned metadata
    assert get_metadata_version(simple_metadata, 1360) == simple_metadata
    assert get_metadata_version(simple_metadata, 1383) == simple_metadata
    assert get_metadata_version(simple_metadata, 1390) == simple_metadata


def test_get_categories():
    """Test get_categories function"""
    # Test valid categorized metadata
    assert get_categories(categorized_metadata)[0]['shared_key'] == 'shared_value'
    assert get_categories(categorized_metadata)[1]['shared_key'] == 'shared_value'

    assert get_categories(categorized_metadata)[0]['key'] == 'first_value'
    assert get_categories(categorized_metadata)[1]['key'] == 'second_value'

    assert get_categories(categorized_metadata)[0]['overwritten_key'] == 'old_value'
    assert get_categories(categorized_metadata)[1]['overwritten_key'] == 'new_value'

    # Test non-categorized metadata
    assert get_categories(simple_metadata) == [simple_metadata]
