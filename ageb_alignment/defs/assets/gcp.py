import geopandas as gpd
import networkx as nx
import pandas as pd
import shapely
from dagster_components.partitions import zone_partitions

import dagster as dg


@dg.op
def get_vertices(gdf: gpd.GeoDataFrame) -> set:
    """Build clique vertices from touching geometries.

    Args:
        gdf: GeoDataFrame with polygon geometries and a ``cvegeo`` identifier.

    Returns:
        Set of frozensets, where each frozenset is a clique with more than
        two touching ``cvegeo`` members.
    """
    points = gdf.sjoin(gdf, how="inner", predicate="touches")

    g = nx.Graph()
    for _, row in points.iterrows():
        g.add_edge(row["cvegeo_left"], row["cvegeo_right"])

    vertices = [
        frozenset(clique) for clique in nx.enumerate_all_cliques(g) if len(clique) > 2
    ]
    return set(vertices)


@dg.op
def get_clique_geometries(gdf: gpd.GeoDataFrame, points: set) -> dict:
    """Compute point intersections for each clique.

    Args:
        gdf: GeoDataFrame containing ``cvegeo`` and ``geometry`` columns.
        points: Set of cliques (frozensets) to evaluate.

    Returns:
        Dictionary mapping each clique to a Shapely ``Point`` intersection.
        Cliques with empty or non-point intersections are excluded.
    """
    series = gdf.set_index("cvegeo")
    series = series["geometry"]

    clique_geometries = {}
    for clique in points:
        geometries = []
        for elem in clique:
            geometry = series.loc[elem]
            geometries.append(geometry)

        intersection = shapely.intersection_all(geometries)
        if intersection.is_empty:
            continue
        if not isinstance(intersection, shapely.geometry.Point):
            continue
        clique_geometries[clique] = intersection
    return clique_geometries


@dg.op(out=dg.Out(io_manager_key="points_manager"))
def merge_columns(source: dict, target: dict) -> pd.DataFrame:
    """Merge source and target control points into GCP table format.

    Args:
        source: Mapping of clique to source Shapely ``Point``.
        target: Mapping of clique to target Shapely ``Point``.

    Returns:
        DataFrame containing map/source coordinates and default adjustment
        columns, deduplicated by coordinate combinations.
    """
    temp_source = pd.Series(source, name="source")
    temp_target = pd.Series(target, name="target")

    return (
        pd.concat([temp_source, temp_target], axis=1)
        .dropna()
        .assign(
            sourceX=lambda df: df["source"].apply(lambda x: x.coords.xy[0][0]),
            sourceY=lambda df: df["source"].apply(lambda x: x.coords.xy[1][0]),
            mapX=lambda df: df["target"].apply(lambda x: x.coords.xy[0][0]),
            mapY=lambda df: df["target"].apply(lambda x: x.coords.xy[1][0]),
            enable=1,
            dX=0,
            dY=0,
            residual=0,
        )[["mapX", "mapY", "sourceX", "sourceY", "enable", "dX", "dY", "residual"]]
        .drop_duplicates(subset=["mapX", "mapY", "sourceX", "sourceY"])
        .drop_duplicates(subset=["sourceX", "sourceY"])
        .drop_duplicates(subset=["mapX", "mapY"])
    )


def initial_gcp_factory(source_year: int, target_year: int) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["gcp", str(source_year)],
        ins={
            "df_source": dg.AssetIn(key=["zone_agebs", "extended", str(source_year)]),
            "df_target": dg.AssetIn(key=["zone_agebs", "extended", str(target_year)]),
        },
        partitions_def=zone_partitions,
        group_name="gcp",
    )
    def _asset(
        df_source: gpd.GeoDataFrame,
        df_target: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        sources = get_vertices(df_source)
        targets = get_vertices(df_target)

        source_geoms = get_clique_geometries(df_source, sources)
        target_geoms = get_clique_geometries(df_target, targets)

        return merge_columns(source_geoms, target_geoms)

    return _asset


initial_gcp_assets = [
    initial_gcp_factory(1990, 2010),
    initial_gcp_factory(2000, 2010),
]
