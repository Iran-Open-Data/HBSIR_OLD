import pandas as pd
import geopandas as gpd

from .core.metadata_reader import metadata, defaults
from . import utils


def create_geoseries(area: str, map_name: str) -> gpd.GeoSeries:
    file_name = metadata.maps[map_name][area]["file_name"]
    path = defaults.maps.joinpath(map_name, file_name)
    column_name = metadata.maps[map_name][area]["code_column"]
    codes = metadata.maps[map_name][area]["code"]
    area_names = metadata.household[area]["name"]
    codes = {value: area_names[key] for key, value in codes.items()}
    if not path.exists():
        utils.download_map(map_name)
    geoseries = (
        gpd.read_file(path)[[column_name, "geometry"]]
        .join(pd.Series(codes, name=area), on=column_name)
        .set_index(area)
        .loc[:, "geometry"]
    )
    return geoseries


def add_geometry(
    table: pd.DataFrame, area: str | None = None, map_name: str = "humandata"
) -> gpd.GeoDataFrame:
    names = table.index.names + table.columns.to_list()
    if area == "Region":
        assert "Region" in names
    elif area == "Province":
        assert "Province" in names
    elif "Region" in names:
        area = "Region"
    elif "Province" in names:
        area = "Province"
    else:
        raise ValueError

    geoseries = create_geoseries(area, map_name)
    geotable = gpd.GeoDataFrame(table.join(geoseries, on=area))
    return geotable
