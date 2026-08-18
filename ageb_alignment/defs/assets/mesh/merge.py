import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg


@dg.asset(
    key=["reprojected", "merged"],
    ins={
        f"agebs_{year}": dg.AssetIn(key=["reprojected", "base", str(year)])
        for year in (1990, 2000, 2010, 2020)
    },
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_manager",
    group_name="reprojected_merged",
)
def merge_meshes(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    merged = agebs_1990.rename(
        columns={
            "pop_fraction": "pop_fraction_1990",
            "geometry": "geometry_1990",
        },
    )

    for year, agebs in zip(
        (2000, 2010, 2020),
        (agebs_2000, agebs_2010, agebs_2020),
        strict=True,
    ):
        temp = agebs[
            [
                "codigo",
                "pop_fraction",
                "geometry",
            ]
        ].rename(
            columns={
                "pop_fraction": f"pop_fraction_{year}",
                "geometry": f"geometry_{year}",
            },
        )
        merged = merged.merge(temp, how="outer", on="codigo")

    for year in (2000, 2010, 2020):
        merged["geometry_1990"] = merged["geometry_1990"].fillna(
            merged[f"geometry_{year}"],
        )
    return merged.drop(
        columns=["geometry_2000", "geometry_2010", "geometry_2020"],
    ).rename(columns={"geometry_1990": "geometry"})
