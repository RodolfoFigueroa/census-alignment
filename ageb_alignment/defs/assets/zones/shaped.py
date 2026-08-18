import subprocess
from pathlib import Path
from typing import cast

import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from ageb_alignment.defs.resources import (
    MapshaperResource,
    PathResource,
    PreferenceResource,
)


def zone_agebs_shaped_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "shaped", str(year)],
        ins={
            "ageb_path": dg.AssetIn(
                key=["zone_agebs", "initial", str(year)],
                input_manager_key="geodataframe_geojson_manager",
            ),
        },
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="shaped",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mapshaper_resource: MapshaperResource,
        path_resource: PathResource,
        preference_resource: PreferenceResource,
        ageb_path: Path,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key
        out_root_path = Path(path_resource.out_path)

        asset_key_path = cast("list[str]", context.asset_key.path)

        out_dir = out_root_path / "/".join(asset_key_path)
        out_dir.mkdir(exist_ok=True, parents=True)

        out_path_json = out_dir / f"{zone}.geojson"

        try:
            mapshaper_resource.clean(ageb_path, out_path_json)
        except subprocess.CalledProcessError as e:
            context.log.exception("Error running mapshaper: %s", e.output)
            raise

        df = gpd.read_file(out_path_json)
        df_orig = gpd.read_file(ageb_path)

        out_path_json.unlink()

        if len(df) != len(df_orig):
            if preference_resource.raise_on_deleted_geometries:
                err = f"Geometries were deleted for zone {zone}."
                raise RuntimeError(err)

            context.log.warning("Geometries for zone %s were deleted", zone)

        return df.to_crs("EPSG:6372")

    return _asset


zone_agebs_clean = [
    zone_agebs_shaped_factory(year) for year in (1990, 2000, 2010, 2020)
]
