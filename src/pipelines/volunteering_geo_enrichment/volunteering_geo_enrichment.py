import logging
from dataclasses import dataclass, asdict
from typing import Optional, cast, List
from datetime import datetime
import requests

from google.cloud.bigquery import Client
from prefect.states import State, Completed
from prefect_gcp import GcpCredentials

from src.common.loaders.bq_loader import wait_for_bigquery_data_to_be_accessible
from src.common.logging.PipelineLogger import PipelineLogger
from src.common.monitoring.flow import monitored_flow
from src.common.variables import secret_value
from src.pipelines.bigquery import bigquery_datalake

# Initialize logging from logging_setup
logger = PipelineLogger("volunteering_geo_enrichment")

GCLOUD_PROJECT_ID = "holi-shared"
GCLOUD_LOCATION = "europe-north1"
GERMANY = "Germany"

CACHE_LOCATION_TABLE = f"{bigquery_datalake.project}.{bigquery_datalake.dataset}.location_cache"
RAW_CLASSIFICATION_TABLE = (
    f"{bigquery_datalake.project}.{bigquery_datalake.dataset}.volunteering_voltastics_raw_deduped"
)

# these city names exist multiple times in Germany, are therefore non-unique and are therefore excluded from enrichment
DUPLICATE_CITY_NAMES = {
    "Neuenkirchen",
    "Schoenberg",
    "House",
    "Neunkirchen",
    "Rohrbach",
    "Mountain",
    "Mountains",
    "Buchholz",
    "Roth",
    "Tiefenbach",
    "Altdorf",
    "Hohenfelde",
    "Holzheim",
    "Königsfeld",
    "Lichtenau",
    "Limbach",
    "Linden",
    "Neukirchen",
    "Oberhausen",
    "Petersberg",
    "Reichenbach",
    "Schönborn",
    "Alsdorf",
    "Altenstadt",
    "Birkenfeld",
    "Boos",
    "Bornheim",
    "Breitscheid",
    "Brunn",
    "Book",
    "Dahlem",
    "Drage",
    "Elmenhorst",
    "Erbach",
    "Eschbach",
    "Falkenberg",
    "Fischbach",
    "Freudenberg",
    "Friedland",
    "Hartmannsdorf",
}


@dataclass
class LocationCacheEntry:
    location: str
    latitude: float
    longitude: float
    created_at: str


class VolunteeringGeoEnricher:
    bq_client: Client
    geoapify_api_key: str

    def __init__(self, geoapify_api_key: str) -> None:
        gcp_credentials = GcpCredentials(project=GCLOUD_PROJECT_ID)
        self.bq_client = gcp_credentials.get_bigquery_client(project=GCLOUD_PROJECT_ID, location=GCLOUD_LOCATION)
        self.geoapify_api_key = geoapify_api_key

    def fetch_coordinates_from_geoapify(self, location: str) -> Optional[LocationCacheEntry]:
        try:
            url = "https://api.geoapify.com/v1/geocode/search"
            params = {"text": f"{location}, {GERMANY}", "apiKey": self.geoapify_api_key}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            features = data.get("features", [])

            if len(features) > 0:
                latitude = features[0]["properties"]["lat"]
                longitude = features[0]["properties"]["lon"]
                returned_country = features[0]["properties"]["country"]
                if returned_country == GERMANY:
                    return LocationCacheEntry(
                        location=location,
                        latitude=latitude,
                        longitude=longitude,
                        created_at=datetime.now().isoformat(),
                    )
                else:
                    return None
            else:
                return None
        except Exception as e:
            logger.warning(f"error fetching coordinates for {location}: {e}")
            return None

    def update_cache_table_batch(self, entries: List[LocationCacheEntry], cache_location_table: str) -> None:
        try:
            table = self.bq_client.get_table(cache_location_table)
            rows_to_insert = [asdict(entry) for entry in entries]
            errors = self.bq_client.insert_rows_json(table, rows_to_insert)

            if errors:
                logger.error(f"error updating cache: {errors}")
            else:
                logger.info(f"cache updated for {len(entries)} locations.")

        except Exception as e:
            logger.error(f"error accessing cache table: {e}")

    @staticmethod
    def _normalized_location_query(location: str) -> str:
        return GERMANY if location.strip() == "Bundesweit" else location.strip()

    def enrich_location_data(self) -> None:
        # query for rows with missing latitude or longitude in raw classification table
        query = f"""
        SELECT location
        FROM `{RAW_CLASSIFICATION_TABLE}`
        WHERE (latitude IS NULL OR longitude IS NULL)
        AND location NOT IN (
            SELECT location 
            FROM `{CACHE_LOCATION_TABLE}`
        )
        AND TIMESTAMP_TRUNC(partitiontime, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
        """  # noqa: S608
        result = self.bq_client.query(query).result()
        rows = [dict(row) for row in result]
        locations_without_duplicate_cities = [
            row["location"] for row in rows if row["location"] not in DUPLICATE_CITY_NAMES
        ]
        normalized_locations_without_duplicate_cities = [
            self._normalized_location_query(location) for location in locations_without_duplicate_cities
        ]

        logger.debug(
            f"found {len(normalized_locations_without_duplicate_cities)} rows with missing latitude or longitude: %s",
            normalized_locations_without_duplicate_cities,
        )

        entries_to_cache = {}

        for location in normalized_locations_without_duplicate_cities:
            if location not in entries_to_cache:
                entry = self.fetch_coordinates_from_geoapify(location)
                if entry is not None:
                    logger.debug(
                        f"retrieved latitude={entry.latitude}, longitude={entry.longitude} for location={entry.location}"
                    )
                    entries_to_cache[location] = entry

        # Batch update the cache after processing all rows
        if entries_to_cache:
            self.update_cache_table_batch(
                cast(List[LocationCacheEntry], entries_to_cache.values()), CACHE_LOCATION_TABLE
            )
        else:
            logger.debug(f"no new location data found to add to location table '{CACHE_LOCATION_TABLE}'")


@monitored_flow
def volunteering_geo_enrichment(wait_for_bq: bool = True) -> State:
    enricher = VolunteeringGeoEnricher(cast(str, secret_value("geoapify-api-key")))
    enricher.enrich_location_data()
    if wait_for_bq:
        wait_for_bigquery_data_to_be_accessible(CACHE_LOCATION_TABLE)
    return Completed()


if __name__ == "__main__":
    logging.getLogger("volunteering_geo_enrichment").setLevel(logging.DEBUG)
    volunteering_geo_enrichment(wait_for_bq=False)  # type: ignore[call-overload]
