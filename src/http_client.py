import base64
import time
from typing import Any

import requests


class ApiError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class HttpClient:
    def __init__(self, base_url: str, api_key: str, api_secret: str, timeout_seconds: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()
        self.default_headers = {
            "Authorization": f"Basic {self._basic_auth(api_key, api_secret)}",
            "Accept": "application/json",
            "User-Agent": "cflt-topic-usage/1.0.0",
        }

    @staticmethod
    def _basic_auth(api_key: str, api_secret: str) -> str:
        token = f"{api_key}:{api_secret}".encode("utf-8")
        return base64.b64encode(token).decode("ascii")

    def request_json(
        self,
        method: str,
        path_or_url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        allow_404: bool = False,
    ) -> Any:
        headers = dict(self.default_headers)
        if extra_headers:
            headers.update(extra_headers)

        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            url = path_or_url
        else:
            url = f"{self.base_url}{path_or_url}"

        attempt = 0
        while True:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise ApiError(f"HTTP request failed: {exc}") from exc
                self._sleep_with_backoff(attempt)
                attempt += 1
                continue

            if response.status_code == 404 and allow_404:
                return None

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                self._sleep_with_backoff(attempt, retry_after=response.headers.get("Retry-After"))
                attempt += 1
                continue

            if response.status_code >= 400:
                details = response.text
                try:
                    body = response.json()
                    details = body
                except ValueError:
                    pass
                raise ApiError(f"API request failed ({response.status_code}): {details}", status_code=response.status_code)

            if not response.text:
                return {}

            try:
                return response.json()
            except ValueError as exc:
                raise ApiError("Expected JSON response but received non-JSON payload") from exc

    @staticmethod
    def _sleep_with_backoff(attempt: int, retry_after: str | None = None) -> None:
        if retry_after and retry_after.isdigit():
            time.sleep(min(int(retry_after), 30))
            return
        time.sleep(min((2 ** attempt) * 0.5, 5.0))
