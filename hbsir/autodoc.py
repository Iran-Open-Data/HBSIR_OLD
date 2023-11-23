import pandas as pd
from pandas.api.types import infer_dtype

from . import api, utils
from .core.metadata_reader import original_tables, defaults, metadata


doc_dir = defaults.root_dir.joinpath("docs")
raw_tables_dir = doc_dir.joinpath("tables", "raw")
raw_tables_dir.mkdir(exist_ok=True, parents=True)
csv_dir = defaults.root_dir.joinpath("temp", "csv")
csv_dir.mkdir(exist_ok=True, parents=True)

def maybe_to_numeric(column: pd.Series) -> pd.Series:
    try:
        column = column.str.replace(r"\s+", "", regex=True).astype("Int64")
    except ValueError:
        try:
            column = column.str.replace(r"\s+", "", regex=True).astype("Float64")
        except ValueError:
            pass
    return column


def create_raw_summary_table(table: pd.DataFrame) -> pd.DataFrame:
    table = clean_raw_data(table)
    rows = len(table.index)
    description = table.isna().sum().to_frame("Missing_Count")
    description["Availability_Ratio"] = (
        description["Missing_Count"].div(rows).sub(1).mul(-100)
    )
    description["Data_Type"] = table.apply(infer_dtype)
    description["Unique_Values"] = table.apply(lambda s: len(s.unique()))
    description["Frequent_Values"] = table.apply(
        lambda s: "; ".join(
            [
                f"{key}: {value:,}"
                for key, value in s.value_counts().head(3).to_dict().items()
            ]
        )
    )
    description.index.name = "Column"
    return description


def clean_raw_data(table: pd.DataFrame) -> pd.DataFrame:
    table = table.replace(r"^\s*$", None, regex=True)
    for col in table.columns:
        if table[col].dtype == "object":
            table[col] = table[col].str.strip()
            table[col] = maybe_to_numeric(table[col])
        elif table[col].dtype == "float64":
            try:
                table[col] = table[col].astype("Int64")
            except (ValueError, TypeError):
                pass
    return table



def collapse_years(table: pd.DataFrame) -> pd.DataFrame:
    table = table.copy()
    last_year = table.index.to_list()[-1]
    filt = - (table == table.shift(1)).all(axis="columns")
    table = table.loc[filt]
    start_year = table.index.to_series()
    end_year = start_year.shift(-1, fill_value=last_year+1) - 1
    new_index = start_year.astype(str)
    filt = start_year != end_year
    new_index.loc[filt] = (new_index.astype(str) + "-" + end_year.astype(str))
    table.index = pd.Index(new_index, name="Years")
    return table


def generate_availability_tables():
    csv_dir.joinpath("availability").mkdir(exist_ok=True, parents=True)
    for table_name in original_tables:
        years = [
            year for _, year in utils.construct_table_year_pairs(table_name, "all")
        ]
        columns = []
        for year in years:
            columns.append(api.load_table(table_name, year, "raw").columns.to_list())
        availability = pd.DataFrame(columns, index=pd.Index(years, name="Year"))

        availability.to_csv(csv_dir.joinpath("availability", f"{table_name}.csv"))


def generate_raw_summary_tables():
    for table_name in original_tables:
        years = [
            year for _, year in utils.construct_table_year_pairs(table_name, "all")
        ]
        for year in years:
            table = api.load_table(table_name, year, "raw")
            summary_table = create_raw_summary_table(table)
            csv_dir.joinpath("raw", table_name).mkdir(exist_ok=True, parents=True)
            summary_table.to_csv(csv_dir.joinpath("raw", table_name, f"{year}.csv"))


def generate_raw_description():
    for table_name in original_tables:
        md_page_content = ""
        md_page_content += f"# {table_name}\n\n"

        md_page_content += "## Table Code\n\n"

        md_page_content += (
            pd.Series(metadata.tables["food"]["file_code"])
            .to_frame("Table Code")
            .to_markdown()
        )
        md_page_content += "\n\n\n"

        years = [
            year for _, year in utils.construct_table_year_pairs(table_name, "all")
        ]

        md_page_content += "## Columns Availability\n\n"
        csv_path = csv_dir.joinpath("availability", f"{table_name}.csv")
        availability = pd.read_csv(csv_path, index_col=0).fillna("").pipe(collapse_years)
        md_page_content += availability.to_markdown()
        md_page_content += "\n\n\n"

        md_page_content += "## Annual Summary Tables\n\n"
        for year in years:
            md_page_content += f"### {year}\n\n"
            csv_path = csv_dir.joinpath("raw", table_name, f"{year}.csv")
            summary_table = pd.read_csv(csv_path, index_col=0).fillna("")
            summary_table.columns = summary_table.columns.str.replace("_", " ")
            md_page_content += summary_table.to_markdown()
            md_page_content += "\n\n\n"
        md_file_path = raw_tables_dir.joinpath(f"{table_name}.md")
        with md_file_path.open(mode="w", encoding="utf-8") as md_file:
            md_file.write(md_page_content)
