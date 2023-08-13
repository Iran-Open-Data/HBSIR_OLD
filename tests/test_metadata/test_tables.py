import pytest

import hbsir

metadata = hbsir.metadata_reader.metadata
defaults = hbsir.metadata_reader.defaults


tables_availability = metadata.tables["yearly_table_availability"]


def get_available_years(table_name: str):
    table_availability = tables_availability[table_name]
    years = hbsir.utils.Argham(
        table_availability,
        default_start=defaults.first_year + 1,
        default_end=defaults.last_year + 1,
    ).get_numbers()
    years = list(years)
    years.sort()
    return years


def build_table_year_pairs(table_name: str):
    available_years = get_available_years(table_name)
    return [(table_name, year) for year in available_years]


@pytest.mark.parametrize("table_name,year", build_table_year_pairs("food"))
def test_load_table(table_name, year):
    hbsir.load_table(
        table_name, year, on_missing="create", recreate=True, save_created=False
    )
