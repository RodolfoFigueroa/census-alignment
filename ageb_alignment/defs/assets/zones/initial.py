import geopandas as gpd
import pandas as pd
from dagster_components.partitions import zone_partitions
from dagster_components.resources import PostGISResource

import dagster as dg


def remove_multipoly(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Removes multipolygons from a GeoDataFrame by exploding them into individual polygons.

    Polygons that are part of a multipolygon are split into separate rows, with
    their population (pobtot) divided equally among the parts and a letter suffix
    appended to their cvegeo identifier (e.g., "0101001" becomes "0101001_A",
    "0101001_B", etc.).

    Args:
        df: A GeoDataFrame containing at least "cvegeo", "pobtot", and geometry
            columns.

    Returns:
        A GeoDataFrame where all multipolygons have been replaced by individual
        polygon rows with adjusted cvegeo identifiers and pobtot values.
    """
    counts = df.explode()["cvegeo"].value_counts()
    idx = counts[counts > 1].to_dict()

    nonrepeated = df[~df["cvegeo"].isin(idx.keys())].explode()

    repeated = []
    for cvegeo in idx:
        temp = (
            df[df["cvegeo"] == cvegeo]
            .explode()
            .assign(
                pobtot=lambda df: df["pobtot"] / counts[cvegeo],
                suffix=[chr(x + 65) for x in range(counts[cvegeo])],
                cvegeo=lambda df: df["cvegeo"] + "_" + df["suffix"],
            )
            .drop(columns=["suffix"])
        )
        repeated.append(temp)

    return gpd.GeoDataFrame(pd.concat([nonrepeated] + repeated, ignore_index=True))


def zone_agebs_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "initial", str(year)],
        partitions_def=zone_partitions,
        io_manager_key="geojson_manager",
        group_name="initial",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        postgis_resource: PostGISResource,
    ) -> gpd.GeoDataFrame:
        if year == 2020:
            query = """
                SELECT census_2020_ageb."CVEGEO", census_2020_ageb."POBTOT", census_2020_ageb."geometry"
                    FROM census_2020_ageb
                INNER JOIN census_2020_mun
                    ON census_2020_ageb."CVE_MUN" = census_2020_mun."CVEGEO"
                WHERE census_2020_mun."CVE_MET" = %(met_zone)s
                """
        else:
            query = f"""
                SELECT census_{year}_ageb."CVEGEO", census_{year}_ageb."POBTOT", census_{year}_ageb."geometry"
                    FROM census_{year}_ageb
                WHERE census_{year}_ageb."CVE_MET" = %(met_zone)s
                """  # noqa: S608 TODO: Parameterize the year in the table name

        with postgis_resource.connect() as conn:
            out = gpd.read_postgis(
                query,
                conn,
                params={"met_zone": context.partition_key},
                geom_col="geometry",
                crs="EPSG:6372",
            )
        return remove_multipoly(out).to_crs("EPSG:4326")

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
