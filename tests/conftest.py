"""
Configuration file for pytest
"""
import pytest

from hbsir import data_engine


@pytest.fixture(scope="session")
def food_1400():
    """Loads 1400 Food Table"""
    return data_engine.read_hbs("food", 1400)


@pytest.fixture(scope="session")
def expenditures_1400():
    """Loads 1400 Expenditure Table"""
    return data_engine.read_hbs("Expenditures", 1400)
