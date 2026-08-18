import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg


@dg.asset(
    key="mesh",
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_manager",
    group_name="mesh",
)
def load_mesh(
    context: dg.AssetExecutionContext, postgis_resource: PostgresResource
) -> gpd.GeoDataFrame:
    with postgis_resource.connect() as conn:
        return gpd.read_postgis(
            """
            SELECT mesh_level_9.* FROM mesh_level_9
            INNER JOIN census_2020_mun
                ON ST_Intersects(mesh_level_9.geometry, census_2020_mun.geometry)
            WHERE census_2020_mun.cve_met = %(zone)s
            """,
            conn,
            params={"zone": context.partition_key},
            geom_col="geometry",
        )


def reprojected_factory(year: int) -> dg.AssetsDefinition:
    if year in (1990, 2000):
        ageb_asset = dg.AssetIn(key=["zone_agebs", "translated", str(year)])
    elif year in (2010, 2020):
        ageb_asset = dg.AssetIn(key=["zone_agebs", "shaped", str(year)])

    @dg.asset(
        key=["reprojected", "base", str(year)],
        ins={"agebs": ageb_asset},
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="reprojected",
    )
    def _asset(
        mesh: gpd.GeoDataFrame,
        agebs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        crs = agebs.crs
        if crs is None:
            err = "CRS of agebs GeoDataFrame is not defined. Cannot reproject to mesh."
            raise ValueError(err)

        mesh = mesh.to_crs(crs)

        agebs = agebs.copy()
        agebs["ageb_area"] = agebs.area
        intersection: gpd.GeoDataFrame = mesh.overlay(agebs, how="intersection").assign(
            area_frac=lambda df: df.area.div(df["ageb_area"]),
            pop_fraction=lambda df: df["area_frac"].mul(df["pobtot"]),
        )

        return (
            intersection.groupby("codigo")
            .agg(
                {
                    "pop_fraction": "sum",
                },
            )
            .reset_index()
            .merge(mesh, how="inner", on="codigo")
            .pipe(lambda df: gpd.GeoDataFrame(df, crs=mesh.crs, geometry="geometry"))
        )

    return _asset


dassets = [reprojected_factory(year) for year in (1990, 2000, 2010, 2020)]
