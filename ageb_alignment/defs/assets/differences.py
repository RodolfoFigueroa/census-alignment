import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg


def differences_factory(start_year: int, end_year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["differences", f"{start_year}_{end_year}"],
        ins={"merged": dg.AssetIn(key=["reprojected", "merged"])},
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="differences",
    )
    def _asset(merged: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        start_col, end_col = f"pop_fraction_{start_year}", f"pop_fraction_{end_year}"

        merged = merged[["codigo", start_col, end_col, "geometry"]].copy()
        merged = merged.dropna(subset=[start_col, end_col], how="all")
        merged = merged[~((merged[start_col].notna()) & (merged[end_col].isna()))]

        merged[start_col] = merged[start_col].fillna(0)
        merged[end_col] = merged[end_col].fillna(0)

        merged = merged[~(merged[[start_col, end_col]] == 0).all(axis=1)]
        merged["difference"] = merged[end_col] - merged[start_col]
        return merged.drop(columns=[start_col, end_col])

    return _asset


differences_assets = [
    differences_factory(start_year, end_year)
    for start_year in (1990, 2000, 2010)
    for end_year in (2000, 2010, 2020)
    if start_year < end_year
]
