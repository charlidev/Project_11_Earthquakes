"""
USGS Earthquake API Client.

Responsible for HTTP communication, request validation,
timeouts, retries and GeoJSON response retrieval.
"""

import time
from datetime import datetime, timezone
from typing import Any

import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class USGSAPIError(Exception):
    """Raised when the USGS API request fails."""


class USGSEarthquakeClient:
    """Client for the USGS Earthquake Catalog API."""

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

        self.session.headers.update({"User-Agent": "Project_11_Earthquakes/0.1"})

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        """Convert timezone-aware datetime to UTC ISO format."""

        if value.tzinfo is None:
            raise ValueError("Datetime must include timezone information.")

        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def _request(self, params: dict[str, Any]) -> dict[str, Any]:

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(BASE_URL, params=params, timeout=self.timeout)

                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt >= self.max_retries:
                        raise USGSAPIError(
                            f"USGS API failed after {attempt + 1} attempts. "
                            f"HTTP {response.status_code}"
                        )

                    retry_after = response.headers.get("Retry-After")

                    try:
                        wait_time = float(retry_after)
                    except (TypeError, ValueError):
                        wait_time = self.backoff_factor * (2**attempt)

                    print(f"HTTP {response.status_code}. Retrying in {wait_time:.1f} seconds...")

                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

                payload = response.json()

                if payload.get("type") != "FeatureCollection":
                    raise USGSAPIError("Unexpected USGS response structure.")

                return payload

            except (requests.Timeout, requests.ConnectionError) as error:
                if attempt >= self.max_retries:
                    raise USGSAPIError(
                        f"USGS connection failed after {attempt + 1} attempts: {error}"
                    ) from error

                wait_time = self.backoff_factor * (2**attempt)

                print(f"Connection error. Retrying in {wait_time:.1f} seconds...")

                time.sleep(wait_time)

            except requests.HTTPError as error:
                raise USGSAPIError(f"USGS HTTP request failed: {error}") from error

        raise USGSAPIError("Maximum number of retries exceeded.")

    def get_earthquakes(
        self,
        starttime: datetime,
        endtime: datetime,
        min_magnitude: float = 2.5,
        max_magnitude: float | None = None,
        bbox: dict[str, float] | None = None,
        limit: int = 100,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve earthquake events within a specified period.

        Parameters
        ----------
        starttime : datetime
            Beginning of the query period.
        endtime : datetime
            End of the query period.
        min_magnitude : float
            Minimum earthquake magnitude.
        max_magnitude : float, optional
            Maximum earthquake magnitude.
        bbox : dict, optional
            Geographic bounding box.
        limit : int
            Maximum records returned (USGS limit: 20,000).
        offset : int, optional
            Starting record offset.
        """

        if starttime >= endtime:
            raise ValueError("starttime must be earlier than endtime.")

        if not 1 <= limit <= 20000:
            raise ValueError("limit must be between 1 and 20,000.")

        if min_magnitude < -2:
            raise ValueError("Invalid minimum magnitude.")

        if max_magnitude is not None and max_magnitude < min_magnitude:
            raise ValueError("max_magnitude must be >= min_magnitude.")

        params = {
            "format": "geojson",
            "starttime": self._format_datetime(starttime),
            "endtime": self._format_datetime(endtime),
            "minmagnitude": min_magnitude,
            "eventtype": "earthquake",
            "orderby": "time",
            "limit": limit,
        }

        if max_magnitude is not None:
            params["maxmagnitude"] = max_magnitude

        if offset is not None:
            if offset < 1:
                raise ValueError("offset must be greater than or equal to 1.")
            params["offset"] = offset

        if bbox:
            params.update(bbox)

        return self._request(params)

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self.session.close()

    def __enter__(self) -> "USGSEarthquakeClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
