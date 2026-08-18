import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import dagster as dg
from ageb_alignment.defs.resources import MapshaperResource


def test_mapshaper_resource_rejects_missing_executable(tmp_path: Path) -> None:
    resource = MapshaperResource(
        executable=str(tmp_path / "missing-mapshaper"),
        expected_version="0.6.100",
    )

    with pytest.raises(RuntimeError, match="Run `npm ci --omit=dev`"):
        resource.setup_for_execution(dg.build_init_resource_context())


def test_mapshaper_resource_rejects_unexpected_version(tmp_path: Path) -> None:
    executable = tmp_path / "mapshaper"
    executable.touch()
    resource = MapshaperResource(
        executable=str(executable),
        expected_version="0.6.100",
    )
    result = subprocess.CompletedProcess([executable], 0, stdout="0.7.51\n")

    with (
        patch("ageb_alignment.defs.resources.subprocess.run", return_value=result),
        pytest.raises(RuntimeError, match=r"Expected Mapshaper 0\.6\.100"),
    ):
        resource.setup_for_execution(dg.build_init_resource_context())


def test_mapshaper_clean_passes_paths_as_unquoted_arguments(tmp_path: Path) -> None:
    executable = tmp_path / "mapshaper"
    input_path = tmp_path / "input with spaces.geojson"
    output_path = tmp_path / "output with spaces.geojson"
    resource = MapshaperResource(
        executable=str(executable),
        expected_version="0.6.100",
    )
    run = MagicMock()

    with patch("ageb_alignment.defs.resources.subprocess.run", run):
        resource.clean(input_path, output_path)

    run.assert_called_once_with(
        [
            str(executable),
            "-i",
            str(input_path),
            "-clean",
            "-o",
            str(output_path),
        ],
        check=True,
    )
