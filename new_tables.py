%load_ext autoreload
%autoreload 3
import hbsir
from hbsir import data_engine, data_cleaner, archive_handler, utils
from hbsir.metadata import metadatas, open_yaml
from hbsir.utils import parse_years


################################################################################
# Reload metadata
metadatas.tables = open_yaml("metadata/tables.yaml")
metadatas.schema = open_yaml("metadata/schema.yaml")

# utils.download_7zip()
# TODO not working on pc
# archive_handler.setup(1395)
# archive_handler.unpack(1395, replace = True)

# data_cleaner.parquet_clean_data('house_specifications', [1396, 1397])
# data_cleaner.parquet_clean_data('household_information', [1396, 1397])
# data_engine._get_parquet('house_specifications', 1396)
################################################################################
# 111 HHBase
final = hbsir.create_table_with_schema(
    {"table_list": ["household_information"], "years": [1396, 1397]}
)
final = data_engine.add_attribute(final, "Province")
final = data_engine.add_attribute(final, "Region")
final
################################################################################
# 112 InfMilkTable
final = hbsir.create_table_with_schema({"table_list": ["food"], "years": [1396, 1397]})
final = data_engine.add_classification(final, levels=[4]).dropna()
final
################################################################################
# 113 Member properties
raw = data_cleaner.load_table_data("members_properties", 1396)
data_cleaner.parquet_clean_data("members_properties", [1396, 1397])
final = hbsir.create_table_with_schema(
    {"table_list": ["members_properties"], "years": [1396, 1397]}
)
df = final
final.assign(
    Kid_Under_15 = df['Age'] < 15,
    Kid_Under_11 = df['Age'] < 11,
    Infant = df['Age'] <= 2,
    Newborn = df['Age'] == 0,
    Potential_Student = (df['Age'] >= 6) & (df['Age'] <= 18),
    Under_18 = df['Age'] <= 18,
    Educated_Parent = (df['Relationship'].isin(['Head', 'Spouse'])) & (df['EduYears'] > 11)
)
################################################################################
# 113 UnEmployted
data_cleaner.parquet_clean_data('members_properties', [1396, 1397])
final = hbsir.create_table_with_schema(
    {"table_list": ["members_properties"], "years": [1396, 1397]}
)
data_engine.TableLoader(table_names = ['members_education'], years = [1396, 1397]).load()

final = (
    final[["ID", "Activity_State"]]
    .groupby(["ID", "Activity_State"])
    .size()
    .reset_index()
    .query(" Activity_State == 'Unemployed'")
    .rename(columns={0: "NunEmployed"})
    .drop(columns="Activity_State")
)
final
################################################################################
# employment income table
raw = data_cleaner.load_table_data("employment_income", 1396)
data_cleaner.parquet_clean_data("employment_income", [1396, 1397])
tbl = data_engine.TableLoader(
    schema={"table_list": ["employment_income"], "years": [1396, 1397]}
)
smth = tbl.load()
