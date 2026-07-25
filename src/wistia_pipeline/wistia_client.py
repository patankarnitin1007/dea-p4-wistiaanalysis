"""Thin client for the Wistia Data API and Stats API (v1).

Docs: https://docs.wistia.com/reference/data-api

Assumption: the stats/events.json endpoint's sort order is not documented
as a guarantee, so incremental filtering does not rely on it - callers get
every event on every page up to max_pages and decide what is "new" by
comparing received_at against their checkpoint. max_pages exists purely as
a safety cap; hitting it is logged as a warning so CloudWatch/SNS can
surface a run that may not have reached the end of a media's event history.
"""
import logging
import time

import requests

logger = logging.getLogger(__name__)

WISTIA_API_BASE = "https://api.wistia.com/v1"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class WistiaAPIError(Exception):
    """Raised for any non-recoverable failure calling the Wistia API."""


class WistiaClient:
    def __init__(self, api_token, base_url=WISTIA_API_BASE, timeout=30, max_retries=5, backoff_factor=2.0):
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_token}"})
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

    def get_media(self, media_id):
        """Media metadata: title, hashed_id, created, updated, duration, etc."""
        return self._get(f"medias/{media_id}.json")

    def get_media_stats(self, media_id):
        """Media-level engagement stats: play_count, play_rate, hours_watched, etc."""
        return self._get(f"stats/medias/{media_id}.json")

    def iter_media_events(self, media_id, per_page=100, max_pages=50):
        """Yields visitor-level play events for a media, one dict per event."""
        page = 1
        while page <= max_pages:
            batch = self._get(
                "stats/events.json",
                params={"media_id": media_id, "page": page, "per_page": per_page},
            )
            if not batch:
                return
            yield from batch
            if len(batch) < per_page:
                return
            page += 1
        logger.warning(
            "Reached max_pages=%d fetching events for media_id=%s; there may be more pages",
            max_pages,
            media_id,
        )

    def _get(self, path, params=None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt > self._max_retries:
                    raise WistiaAPIError(f"Request to {url} failed after {attempt} attempts: {exc}") from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code == 200:
                return response.json()
            if response.status_code == 401:
                raise WistiaAPIError(f"Unauthorized calling {url}: check the Wistia API token permissions")
            if response.status_code == 404:
                raise WistiaAPIError(f"Not found calling {url} with params={params}")
            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt > self._max_retries:
                    raise WistiaAPIError(
                        f"{url} failed with status {response.status_code} after {attempt} attempts: {response.text}"
                    )
                self._sleep_before_retry(attempt, retry_after=response.headers.get("Retry-After"))
                continue
            raise WistiaAPIError(f"Unexpected status {response.status_code} calling {url}: {response.text}")

    def _sleep_before_retry(self, attempt, retry_after=None):
        delay = float(retry_after) if retry_after else self._backoff_factor**attempt
        logger.warning("Retrying request in %.1fs (attempt %d/%d)", delay, attempt, self._max_retries)
        time.sleep(delay)
