# HBSIR <img src='https://github.com/Iran-Open-Data/HBSIR/assets/36173945/af8a7d40-d610-42e2-b6b4-c220f7430df4' align="right" height="139" />

A package to obtain household expenditure and income survey data.

## Usage

:one: Clone the repository:

```sh
git clone https://github.com/iran-open-data/HBSIR household
```

:two: Create the `hbsir-settings.yaml` file based on the `settings-sample.yaml`.

:three: Create a script in the root folder, using the `hbsir` modules as needed:

```python
from hbsir import data_engine, data_cleaner, archive_handler, utils

# Downloads 7zip in appropriate directory for you
utils.download_7zip()

# Downloads raw survey data for year(s)
archive_handler.setup(1397)

# Find each table name from tables.yaml
data_cleaner.parquet_clean_data('household_information', 1397)

# Access data at each step of the way
raw = data_cleaner.load_table_data('household_information', 1397)
processed = data_engine.read_table('household_information', 1397)
final = data_engine.load_table('household_information', 1397)
```
