from hbsir import archive_handler, data_cleaner, data_engine, metadata
import pytest


def test_zero_division():
    with pytest.raises(ZeroDivisionError):
        1 / 0
