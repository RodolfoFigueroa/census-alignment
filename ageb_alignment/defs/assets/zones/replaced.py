import functools
import operator

import geopandas as gpd
import numpy as np
from cfc_dagster_utils.partitions import zone_partitions

from ageb_alignment.configs.replacement import (
    replace_1990_2000,
    replace_1990_2010,
    replace_2000_2010,
)
from dagster import AssetExecutionContext, AssetIn, asset


def replace_geoms(
    gdf_old: gpd.GeoDataFrame,
    gdf_new: gpd.GeoDataFrame,
    replace_list: list,
    *,
    check_complete: bool = False,
) -> gpd.GeoDataFrame:
    """Replaces geometries in old with the union of geometries in new following the
    corresponding relations in replace_list."""

    gdf_old = gdf_old.copy()

    # Check if replacement covers all GDF without repeated indices
    old_idx = []
    for oi, _ in replace_list:
        if isinstance(oi, str):
            old_idx.append(oi)
        else:
            old_idx += oi
    if check_complete and np.all(sorted(old_idx) != sorted(gdf_old.index)):
        err = (
            "Replacement list does not cover all geometries in gdf_old. "
            "This may result in unexpected behavior."
        )
        raise ValueError(err)

    # Check targets are unique
    targets = [tl for _, tl in replace_list]
    targets_flat = functools.reduce(operator.iadd, targets, [])

    if len(np.unique(targets_flat)) != len(targets_flat):
        err = (
            "Targets in replace_list are not unique. "
            "This may result in unexpected behavior."
        )
        raise ValueError(err)

    for old_id, new_ids in replace_list:
        if isinstance(old_id, str):
            # This is a one to one or one to many relation
            gdf_old.loc[old_id, "geometry"] = gdf_new.loc[
                new_ids,
                "geometry",
            ].union_all()
        elif isinstance(old_id, list):
            # This is many to one relation or many to many
            pobsum = gdf_old.loc[old_id, "pobtot"].sum()
            gdf_old = gdf_old.drop(old_id)
            new_geom = gdf_new.loc[new_ids, "geometry"].union_all()
            gdf_old.loc["+".join(old_id)] = [pobsum, new_geom]
        else:
            raise NotImplementedError

    return gdf_old.sort_index()


@asset(
    key=["zone_agebs", "replaced", "2000"],
    ins={
        "agebs_old": AssetIn(["zone_agebs", "initial", "2000"]),
        "agebs_new": AssetIn(["zone_agebs", "initial", "2010"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_geojson_manager",
    group_name="replaced",
)
def zones_replaced_2000(
    context: AssetExecutionContext,
    agebs_old: gpd.GeoDataFrame,
    agebs_new: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    agebs_old = agebs_old.set_index("CVEGEO")
    agebs_new = agebs_new.set_index("CVEGEO")

    if zone in replace_2000_2010:
        agebs_replaced = replace_geoms(agebs_old, agebs_new, replace_2000_2010[zone])

        msg = f"Replaced {zone} with 2010 AGEBs."
        context.log.info(msg)
    else:
        agebs_replaced = agebs_old

    return agebs_replaced


@asset(
    key=["zone_agebs", "replaced", "1990"],
    ins={
        "agebs_1990": AssetIn(["zone_agebs", "switched", "1990"]),
        "agebs_2000": AssetIn(["zone_agebs", "initial", "2000"]),
        "agebs_2010": AssetIn(["zone_agebs", "initial", "2010"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_geojson_manager",
    group_name="replaced",
)
def zones_replaced_1990(
    context: AssetExecutionContext,
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    agebs_1990 = agebs_1990.set_index("CVEGEO")
    agebs_2000 = agebs_2000.set_index("CVEGEO")
    agebs_2010 = agebs_2010.set_index("CVEGEO")

    if zone in replace_1990_2010:
        agebs_replaced = replace_geoms(agebs_1990, agebs_2010, replace_1990_2010[zone])

        msg = f"Replaced {zone} with 2010 AGEBs."
        context.log.info(msg)
    elif zone in replace_1990_2000:
        agebs_replaced = replace_geoms(agebs_1990, agebs_2000, replace_1990_2000[zone])

        msg = f"Replaced {zone} with 2000 AGEBs."
        context.log.info(msg)
    else:
        agebs_replaced = agebs_1990

    return agebs_replaced


zones_replace_assets = [zones_replaced_1990, zones_replaced_2000]
