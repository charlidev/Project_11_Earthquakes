"""
USGS Earthquake API Client.

Handles HTTP requests, retries, earthquake counts,
pagination and incremental update filters.
"""

import time
from datetime import datetime, timezone
from typing import Any

import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1"
QUERY_URL = f"{BASE_URL}/query"
COUNT_URL = f"{BASE_URL}/count"

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

VALID_ORDER_OPTIONS = {
    "time",
    "time-asc",
    "magnitude",
    "magnitude-asc",
}


class USGSAPIError(Exception):
    """Raised when the USGS API request fails."""


class USGSEarthquakeClient:
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 4,
        backoff_factor: float = 1.5,
        session: requests.Session | None = None,
    ) -> None:

        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.session = session or requests.Session()

        self.session.headers.update({"User-Agent": "Project_11_Earthquakes/0.2"})

    @staticmethod
    def _format_datetime(value: datetime) -> str:

        if value.tzinfo is None:
            raise ValueError("Datetime must include timezone information.")

        return (
            value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "")
        )

    def _request(
        self,
        url: str,
        params: dict[str, Any],
    ) -> requests.Response:

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                )

                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt >= self.max_retries:
                        raise USGSAPIError(f"Maximum retries exceeded. HTTP {response.status_code}")

                    retry_after = response.headers.get("Retry-After")

                    try:
                        wait_time = float(retry_after)
                    except (TypeError, ValueError):
                        wait_time = self.backoff_factor * (2**attempt)

                    print(f"HTTP {response.status_code}. Retrying in {wait_time:.1f} seconds...")

                    time.sleep(wait_time)
                    continue

                if response.status_code != 204:
                    response.raise_for_status()

                return response

            except (requests.Timeout, requests.ConnectionError) as error:
                if attempt >= self.max_retries:
                    raise USGSAPIError(
                        f"Connection failed after {attempt + 1} attempts: {error}"
                    ) from error

                wait_time = self.backoff_factor * (2**attempt)

                print(f"Connection error. Retrying in {wait_time:.1f} seconds...")

                time.sleep(wait_time)

            except requests.HTTPError as error:
                raise USGSAPIError(f"USGS HTTP request failed: {error}") from error

        raise USGSAPIError("Request failed.")

    def _build_filters(
        self,
        starttime: datetime,
        endtime: datetime,
        min_magnitude: float = 2.5,
        updatedafter: datetime | None = None,
        include_deleted: bool = False,
        bbox: dict[str, float] | None = None,
    ) -> dict[str, Any]:

        if starttime >= endtime:
            raise ValueError("starttime must be earlier than endtime.")

        params = {
            "starttime": self._format_datetime(starttime),
            "endtime": self._format_datetime(endtime),
            "minmagnitude": min_magnitude,
            "eventtype": "earthquake",
        }

        if updatedafter is not None:
            params["updatedafter"] = self._format_datetime(updatedafter)

        if include_deleted:
            params["includedeleted"] = "true"

        if bbox:
            params.update(bbox)

        return params

    def count_earthquakes(
        self,
        starttime: datetime,
        endtime: datetime,
        min_magnitude: float = 2.5,
        updatedafter: datetime | None = None,
    ) -> int:
        """Return the number of events matching the specified filters."""

        params = self._build_filters(
            starttime,
            endtime,
            min_magnitude,
            updatedafter,
        )

        params["format"] = "text"

        response = self._request(COUNT_URL, params)

        if response.status_code == 204:
            return 0

        try:
            return int(response.text.strip())

        except ValueError as error:
            raise USGSAPIError(f"Unexpected count response: {response.text!r}") from error

    def get_earthquakes(
        self,
        starttime: datetime,
        endtime: datetime,
        min_magnitude: float = 2.5,
        max_magnitude: float | None = None,
        bbox: dict[str, float] | None = None,
        limit: int = 100,
        offset: int = 1,
        updatedafter: datetime | None = None,
        include_deleted: bool = False,
        orderby: str = "time-asc",
    ) -> dict[str, Any]:
        """Retrieve a paginated GeoJSON response."""

        if not 1 <= limit <= 20000:
            raise ValueError("limit must be between 1 and 20,000.")

        if offset < 1:
            raise ValueError("offset must start at 1.")

        if orderby not in VALID_ORDER_OPTIONS:
            raise ValueError(f"Invalid orderby value: {orderby}")

        params = self._build_filters(
            starttime,
            endtime,
            min_magnitude,
            updatedafter,
            include_deleted,
            bbox,
        )

        if max_magnitude is not None:
            params["maxmagnitude"] = max_magnitude

        params.update(
            {
                "format": "geojson",
                "limit": limit,
                "offset": offset,
                "orderby": orderby,
            }
        )

        response = self._request(QUERY_URL, params)

        if response.status_code == 204:
            return {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {"count": 0},
            }

        payload = response.json()

        if payload.get("type") != "FeatureCollection":
            raise USGSAPIError("Unexpected response: expected FeatureCollection.")

        return payload

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "USGSEarthquakeClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
