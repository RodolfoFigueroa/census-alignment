# AGEB alignment

## First steps

This project requires an active census database produced with the [`census_processing`](https://github.com/rodolfoFigueroa/census_processing) repository.

Install the locked Python and Node.js dependencies:

```
uv sync
npm ci --omit=dev
```

Node.js 22.23.2 is the supported runtime version. Mapshaper is installed locally
from `package-lock.json`; asset execution never downloads packages. Set
`MAPSHAPER_BIN` only when a deployment provides the pinned Mapshaper executable
at a different location.

Copy the example environment file:

```
cp .env.example .env
```

In the newly created `.env` file, fill in the missing variables as follows:

* `DATA_PATH`: Path to the previously downloaded data directory. The produced files will also be stored here.
* `DAGSTER_HOME`: Path to the `dagster/` directory inside this repository.

Start the Dagster webui

```
uv run dg dev
```

You can visit it at `localhost:3000`


## Usage

By clicking **Materialize all** on the top-right corner, a dialogue will pop up allowing you to choose which metropolitan zones you want to calculate, based on their `CVE_MET` code. All resultant files will be placed inside the `DATA_PATH/generated` directory. Briefly, they consist of the following:

* `zone_agebs/initial/<year>`: Initial unmodified AGEBs for each censal year.
* `zone_agebs/shaped/<year>`: AGEBs processed by `mapshaper`, to simplify and correct possible geometrical errors.
* `zone_agebs/extended/<year>`: AGEBs with an additional "exterior" polygon added. This polygon helps with calculating initial ground control points (GCP).
* `zone_agebs/translated/<year>`: AGEBs after being transformed using GCPs.
* `reprojected/base/<year>`: Aligned population counts reprojected to INEGI's geostatistical mesh.
* `reprojected/merged`: Same reprojected population counts as the previous step, but with all years inside a single database.
* `differences/<year>`: Mesh population loss/gain for each possible year combination.
