import os
from pathlib import Path

import toml
from cfc_dagster_utils.managers.dataframe import DataFrameFileManager
from cfc_dagster_utils.managers.geodataframe import GeoDataFrameFileManager
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from ageb_alignment.defs.resources import (
    AgebDictResource,
    AgebListResource,
    AgebNestedDictResource,
    MapshaperResource,
    PathResource,
    PreferenceResource,
)

# Resources
project_root = Path(__file__).parents[1]
opath = project_root / "data" / "output"
path_resource = PathResource(out_path=str(opath))

mapshaper_executable = project_root / "node_modules" / ".bin" / "mapshaper"
if os.name == "nt":
    mapshaper_executable = mapshaper_executable.with_suffix(".cmd")
mapshaper_resource = MapshaperResource(
    executable=os.getenv("MAPSHAPER_BIN", str(mapshaper_executable)),
    expected_version="0.6.100",
)

postgis_resource = PostgresResource(
    host=dg.EnvVar("POSTGRES_HOST"),
    port=dg.EnvVar("POSTGRES_PORT"),
    user=dg.EnvVar("POSTGRES_USER"),
    password=dg.EnvVar("POSTGRES_PASSWORD"),
    db=dg.EnvVar("POSTGRES_DB"),
)

with Path("./configs/overlaps.toml").open(encoding="utf8") as f:
    overlap_list = toml.load(f)
overlap_list = {f"ageb_{key}": value for key, value in overlap_list.items()}
overlap_resource = AgebListResource(**overlap_list)

with Path("./configs/remove_from_mun.toml").open(encoding="utf8") as f:
    remove_from_mun_list = toml.load(f)
remove_from_mun_list = {
    f"ageb_{key}": value for key, value in remove_from_mun_list.items()
}
remove_from_mun_resource = AgebDictResource(**remove_from_mun_list)

with Path("./configs/preferences.toml").open(encoding="utf8") as f:
    preferences = toml.load(f)
preference_resource = PreferenceResource(
    raise_on_deleted_geometries=preferences["raise_on_deleted_geometries"],
    mesh_level=preferences["mesh_level"],
)

with Path("./configs/switches.toml").open(encoding="utf8") as f:
    switch_list = toml.load(f)
switch_list = {f"ageb_{key}": value for key, value in switch_list.items()}
switch_resource = AgebNestedDictResource(**switch_list)


with Path("./configs/affine.toml").open(encoding="utf8") as f:
    rigid_list = toml.load(f)
rigid_list = {f"ageb_{key}": value for key, value in rigid_list.items()}
affine_resource = AgebDictResource(**rigid_list)

# Definition
definitions = dg.Definitions.merge(
    dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
    dg.Definitions(
        resources={
            "path_resource": path_resource,
            "mapshaper_resource": mapshaper_resource,
            "geodataframe_manager": GeoDataFrameFileManager(
                path_resource=path_resource, extension=".geoparquet"
            ),
            "geodataframe_geojson_manager": GeoDataFrameFileManager(
                path_resource=path_resource, extension=".geojson"
            ),
            "points_manager": DataFrameFileManager(
                path_resource=path_resource, extension=".points", engine="csv"
            ),
            "overlap_resource": overlap_resource,
            "preference_resource": preference_resource,
            "remove_from_mun_resource": remove_from_mun_resource,
            "affine_resource": affine_resource,
            "switch_resource": switch_resource,
            "postgis_resource": postgis_resource,
        },
    ),
)
