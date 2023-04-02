"""
Configuration file for pytest
"""
import pytest

from hbsir import data_engine


@pytest.fixture(scope="session")
def food_1400():
    """Loads 1400 Food Table"""
    return data_engine.load_table("food", 1400)


@pytest.fixture(scope="session")
def expenditures_1400():
    """Loads 1400 Expenditures Table"""
    return data_engine.load_table("Expenditures", 1400)


@pytest.fixture(scope="session")
def incomes_1400():
    """Loads 1400 Incomes Table"""
    return data_engine.load_table("Incomes", 1400)
