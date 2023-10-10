import pytest

import hbsir
from hbsir.metadata_reader import defaults, metadata


tables_availability = metadata.tables["yearly_table_availability"]


def get_available_years(table_name: str):
    table_availability = tables_availability[table_name]
    years = hbsir.utils.Argham(
        table_availability,
        default_start=defaults.first_year,
        default_end=defaults.last_year + 1,
    ).get_numbers()
    years = list(years)
    years.sort()
    return years


def build_table_year_pairs(table_name: str):
    available_years = get_available_years(table_name)
    return [(table_name, year) for year in available_years]


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("household_information")
)
def test_load_household_information(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("members_properties")
)
def test_load_members_properties(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("house_specifications")
)
def test_load_house_specifications(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("food"))
def test_load_food(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("tobacco"))
def test_load_tobacco(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("cloth"))
def test_load_cloth(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("home"))
def test_load_home(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("furniture"))
def test_load_furniture(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("medical"))
def test_load_medical(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("transportation"))
def test_load_transportation(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("communication"))
def test_load_communication(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("entertainment"))
def test_load_entertainment(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("education"))
def test_load_education(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("hotel"))
def test_load_hotel(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("durable"))
def test_load_durable(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("investment"))
def test_load_investment(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("employment_income"))
def test_load_employment_income(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("self_employed_income")
)
def test_load_self_employed_income(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("other_income"))
def test_load_other_income(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("subsidy"))
def test_load_subsidy(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("public_employment_income")
)
def test_load_public_employment_income(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)


@pytest.mark.parametrize(
    "table_name,year", build_table_year_pairs("private_employment_income")
)
def test_load_private_employment_income(table_name, year):
    hbsir.load_table(table_name, year, recreate=True, save_created=False)
