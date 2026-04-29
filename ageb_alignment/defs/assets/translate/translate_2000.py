from pathlib import Path

import geopandas as gpd
import pandas as pd
from dagster_components.partitions import zone_partitions

import dagster as dg
from ageb_alignment.defs.assets.translate.common import (
    generate_options_str,
    get_gcp_fallback,
    translate_geometries_single,
)
from ageb_alignment.defs.resources import PathResource


def translated_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "translated", str(year)],
        ins={
            "ageb_path": dg.AssetIn(
                ["zone_agebs", "shaped", str(year)],
                input_manager_key="path_gpkg_manager",
            ),
            "gcp_automatic": dg.AssetIn(key=["gcp", str(year)]),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name="translated",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
        ageb_path: Path,
        gcp_automatic: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        gcp_final_path = (
            Path(path_resource.data_path)
            / "intermediate"
            / "gcp"
            / str(year)
            / f"{zone}.points"
        )
        gcp, transform_options = get_gcp_fallback(
            gcp_final_path,
            gcp_automatic,
            year,
            context=context,
        )

        options_str = generate_options_str(gcp, transform_options)
        return translate_geometries_single(ageb_path, options_str)

    return _asset


dassets = [translated_factory(1990), translated_factory(2000)]
