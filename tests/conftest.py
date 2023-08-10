"""
Configuration file for pytest
"""
import pytest

import hbsir


@pytest.fixture(scope="session")
def food_1400():
    """Loads 1400 Food Table"""
    return hbsir.load_table("food", 1400, on_missing="download")


@pytest.fixture(scope="session")
def expenditures_1400():
    """Loads 1400 Expenditures Table"""
    hbsir.setup(1400)
    return hbsir.load_table("Expenditures", 1400, on_missing="error")


@pytest.fixture(scope="session")
def weight_1400():
    """Loads 1400 Sampling Weight Table"""
    return hbsir.load_table("Weights", years=[1400])


@pytest.fixture(scope="session")
def incomes_1400():
    """Loads 1400 Incomes Table"""
    return hbsir.load_table("Incomes", 1400)
