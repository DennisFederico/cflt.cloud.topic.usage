from typing import Any
from urllib.parse import quote

from http_client import HttpClient


UNKNOWN_OWNER = "unknown"


class CatalogApiClient:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def get_topic_owner(self, cluster_id: str, topic_name: str) -> str:
        qualified_name = quote(f"{cluster_id}:{topic_name}", safe="")
        response = self.http.request_json(
            method="GET",
            path_or_url=f"/catalog/v1/entity/type/kafka_topic/name/{qualified_name}",
            allow_404=True,
        )
        if response is None:
            return UNKNOWN_OWNER

        owner = self._extract_owner(response)
        return owner or UNKNOWN_OWNER

    def get_topic_owners(self, cluster_id: str, topic_names: list[str]) -> dict[str, str]:
        owners: dict[str, str] = {}
        for topic_name in topic_names:
            owners[topic_name] = self.get_topic_owner(cluster_id, topic_name)
        return owners

    def _extract_owner(self, payload: Any) -> str | None:
        candidate_paths = [
            ("entity", "attributes", "owner"),
            ("attributes", "owner"),
            ("entity", "owner"),
            ("owner",),
        ]
        for path in candidate_paths:
            value = self._get_path(payload, path)
            owner = self._stringify_owner(value)
            if owner:
                return owner

        return self._search_owner_recursive(payload)

    @staticmethod
    def _get_path(data: Any, path: tuple[str, ...]) -> Any:
        current = data
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _search_owner_recursive(self, value: Any) -> str | None:
        if isinstance(value, dict):
            for key, sub_value in value.items():
                if key.lower() in {"owner", "owners", "data_owner", "topic_owner"}:
                    owner = self._stringify_owner(sub_value)
                    if owner:
                        return owner
                owner = self._search_owner_recursive(sub_value)
                if owner:
                    return owner
        elif isinstance(value, list):
            for item in value:
                owner = self._search_owner_recursive(item)
                if owner:
                    return owner
        return None

    @staticmethod
    def _stringify_owner(value: Any) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for key in ("name", "displayName", "display_name", "id"):
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        if isinstance(value, list):
            flattened = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    flattened.append(item.strip())
                elif isinstance(item, dict):
                    nested = CatalogApiClient._stringify_owner(item)
                    if nested:
                        flattened.append(nested)
            if flattened:
                return ",".join(flattened)
        return None
