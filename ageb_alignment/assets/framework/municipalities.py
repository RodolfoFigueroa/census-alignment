from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple, CensusTuple
from dagster import asset
from pathlib import Path


@asset
def municipalities_2000(
    path_resource: PathResource, geometry_2000: GeometryTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/municipalities"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2000.mun.drop(columns=["OID", "LAYER", "NOM_MUN"])
        .rename(columns={"CVEMUNI": "CVEGEO"})
        .assign(
            CVE_ENT=lambda df: df.CVEGEO.str[:2].astype(int),
            CVE_MUN=lambda df: df.CVEGEO.str[2:].astype(int),
            CVEGEO=lambda df: df.CVEGEO.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
    )
    merged.to_file(out_path / "2000.gpkg")


@asset
def municipalities_2010(
    path_resource: PathResource, geometry_2010: GeometryTuple, census_2010: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/municipalities"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2010.mun.drop(columns=["OID", "NOM_MUN"])
        .assign(
            CVEGEO=lambda df: (df.CVE_ENT + df.CVE_MUN).astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(census_2010.mun)
    )
    merged.to_file(out_path / "2010.gpkg")


@asset
def municipalities_2020(
    path_resource: PathResource, geometry_2020: GeometryTuple, census_2020: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/municipalities"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2020.mun.drop(columns="NOMGEO")
        .assign(
            CVEGEO=lambda df: df.CVEGEO.astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(census_2020.mun)
    )
    merged.to_file(out_path / "2020.gpkg")
