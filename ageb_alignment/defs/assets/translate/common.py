import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from osgeo import gdal

from dagster import AssetExecutionContext

gdal.UseExceptions()


def generate_options_str(gcp: np.ndarray, transform_options: str) -> str:
    """Generates a GDAL options string with GCPs and target CRS.

    Args:
        gcp: A 2D array of ground control points with columns
            [sourceX, sourceY, mapX, mapY].
        transform_options: Additional GDAL transform options to prepend
            (e.g. ``"-tps"`` or ``"-order 1"``).

    Returns:
        A string of GDAL options including the target CRS (EPSG:6372) and
        all GCPs formatted as ``-gcp sourceX sourceY mapX mapY`` entries.
    """
    options_str = transform_options + " -t_srs EPSG:6372 "
    for row in gcp:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "
    return options_str


def load_gcp(gcp_path: Path) -> np.ndarray:
    points = pd.read_csv(
        gcp_path,
        usecols=["sourceX", "sourceY", "mapX", "mapY"],
        header=1,
    )
    return points[
        ["sourceX", "sourceY", "mapX", "mapY"]
    ].to_numpy()  # Ensure right order


def translate_geometries_single(ageb_path: Path, options: str) -> gpd.GeoDataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        out_path = Path(temp_dir) / "df.gpkg"
        gdal.VectorTranslate(str(out_path), str(ageb_path), options=options)
        return gpd.read_file(out_path)


def translate_geometries_double(
    ageb_path: Path,
    options_first: str,
    options_second: str,
) -> gpd.GeoDataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        temp_path = temp_dir_path / "df_temp.gpkg"
        out_path = temp_dir_path / "df.gpkg"

        gdal.VectorTranslate(str(temp_path), str(ageb_path), options=options_first)
        gdal.VectorTranslate(str(out_path), str(temp_path), options=options_second)

        return gpd.read_file(out_path)


def get_gcp_fallback(
    gcp_path: Path,
    gcp_automatic: pd.DataFrame,
    year: int,
    *,
    context: AssetExecutionContext | None,
) -> tuple[np.ndarray, str]:
    if gcp_path.exists():
        gcp = load_gcp(gcp_path)
        options = "-tps"
        infix = "manual"
    else:
        gcp = gcp_automatic[["sourceX", "sourceY", "mapX", "mapY"]].to_numpy()
        options = "-order 1"
        infix = "automatic"
    if context is not None:
        msg = f"Used {infix} GCP for {context.partition_key}/{year} with {options}."
        context.log.info(
            msg,
        )
    return gcp, options
