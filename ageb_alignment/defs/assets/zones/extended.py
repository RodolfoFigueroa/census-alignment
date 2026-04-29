import geopandas as gpd
import pandas as pd
import shapely
from dagster_components.partitions import zone_partitions

from dagster import AssetIn, AssetsDefinition, asset


def get_outer_polygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Build an outer polygon ring around all geometries in a GeoDataFrame.

    The function creates a bounding box from the input geometries, expands it,
    subtracts the merged input geometry, and keeps the largest resulting
    polygon as the outer area.

    Args:
        gdf: Input GeoDataFrame containing a ``geometry`` column.

    Returns:
        A single-row GeoDataFrame with ``cvegeo='OUT'`` and the computed outer
        polygon geometry.
    """

    box = shapely.box(*gdf.total_bounds)
    box = shapely.buffer(box, 100)

    merged = shapely.unary_union(gdf["geometry"])
    cut = box.difference(merged)

    if isinstance(cut, shapely.geometry.Polygon):
        max_poly = cut
    else:
        max_area, max_poly = 0, None
        for poly in cut.geoms:
            area = poly.area
            if area > max_area:
                max_area = area
                max_poly = poly

    return gpd.GeoDataFrame(
        ["OUT"],
        columns=["cvegeo"],
        geometry=[max_poly],
        crs=gdf.crs,
    )  # ty:ignore[no-matching-overload]


def zones_extended_factory(year: int) -> AssetsDefinition:
    @asset(
        key=["zone_agebs", "extended", str(year)],
        ins={
            "agebs": AssetIn(key=["zone_agebs", "shaped", str(year)]),
        },
        io_manager_key="gpkg_manager",
        partitions_def=zone_partitions,
        group_name="extended",
    )
    def _asset(agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        agebs = agebs.assign(geometry=lambda df: df["geometry"].make_valid())
        outer_poly = get_outer_polygon(agebs)
        return gpd.GeoDataFrame(pd.concat([agebs, outer_poly], ignore_index=True))

    return _asset


zones_extended = [zones_extended_factory(year) for year in (1990, 2000, 2010)]
