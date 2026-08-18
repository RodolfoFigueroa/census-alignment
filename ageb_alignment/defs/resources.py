import subprocess
from pathlib import Path

import dagster as dg


class PreferenceResource(dg.ConfigurableResource):
    raise_on_deleted_geometries: bool
    mesh_level: int


class PathResource(dg.ConfigurableResource):
    out_path: str


class MapshaperResource(dg.ConfigurableResource):
    executable: str
    expected_version: str

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        del context
        executable = Path(self.executable)
        if not executable.is_file():
            msg = (
                f"Mapshaper executable not found at {executable}. "
                "Run `npm ci --omit=dev` from the project root or set "
                "MAPSHAPER_BIN to an installed Mapshaper executable."
            )
            raise RuntimeError(msg)

        try:
            result = subprocess.run(  # noqa: S603
                [executable, "-version"],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            msg = f"Could not execute Mapshaper at {executable}: {error}"
            raise RuntimeError(msg) from error

        version = result.stdout.strip()
        if version != self.expected_version:
            msg = (
                f"Expected Mapshaper {self.expected_version}, found {version!r} at "
                f"{executable}. Run `npm ci --omit=dev` to restore the locked version."
            )
            raise RuntimeError(msg)

    def clean(self, input_path: Path, output_path: Path) -> None:
        subprocess.run(  # noqa: S603
            [
                self.executable,
                "-i",
                str(input_path),
                "-clean",
                "-o",
                str(output_path),
            ],
            check=True,
        )


class AgebListResource(dg.ConfigurableResource):
    ageb_1990: list[list[str]] | None = None
    ageb_2000: list[list[str]] | None = None
    ageb_2010: list[list[str]] | None = None
    ageb_2020: list[list[str]] | None = None


class AgebDictResource(dg.ConfigurableResource):
    ageb_1990: dict[str, list] | None = None
    ageb_2000: dict[str, list] | None = None
    ageb_2010: dict[str, list] | None = None
    ageb_2020: dict[str, list] | None = None


class AgebNestedDictResource(dg.ConfigurableResource):
    ageb_1990: dict[str, dict[str, list]] | None = None
    ageb_2000: dict[str, dict[str, list]] | None = None
    ageb_2010: dict[str, dict[str, list]] | None = None
    ageb_2020: dict[str, dict[str, list]] | None = None
