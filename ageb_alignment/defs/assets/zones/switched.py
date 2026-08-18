import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions

import dagster as dg
from ageb_alignment.defs.resources import AgebDictResource


def zones_switched_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "switched", str(year)],
        ins={
            "all_agebs": dg.AssetIn(
                key=["zone_agebs", "initial", str(year)],
                partition_mapping=dg.AllPartitionMapping(),
            ),
        },
        io_manager_key="geodataframe_geojson_manager",
        partitions_def=zone_partitions,
        group_name="switched",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        switch_resource: AgebDictResource,
        all_agebs: dict[str, gpd.GeoDataFrame],
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key
        agebs = all_agebs[zone]
        switches: dict = getattr(switch_resource, f"ageb_{year}")

        # Zone is giving AGEBs
        if zone in switches:
            dropped_agebs = []
            for ageb_list in switches[zone].values():
                dropped_agebs.extend(ageb_list)
            agebs = agebs[~agebs["CVEGEO"].isin(dropped_agebs)]

            msg = f"Dropped AGEBs {dropped_agebs} from zone {zone}."
            context.log.info(msg)

        # Zone is receiving AGEBs
        for giver, receiver_map in switches.items():
            for receiver, wanted_agebs in receiver_map.items():
                if zone == receiver:
                    giver_df = all_agebs[giver]
                    giver_agebs = giver_df[giver_df["CVEGEO"].isin(wanted_agebs)]
                    agebs = pd.concat([agebs, giver_agebs])

                    msg = f"Received AGEBs {wanted_agebs} from zone {giver}."
                    context.log.info(
                        msg,
                    )

        return agebs

    return _asset


zones_switched_assets = [zones_switched_factory(year) for year in (1990,)]
