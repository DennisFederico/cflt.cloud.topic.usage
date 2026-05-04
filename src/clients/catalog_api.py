from typing import Any
from urllib.parse import quote

from http_client import HttpClient


UNKNOWN_OWNER = "unknown"
UNKNOWN_OWNER_EMAIL = "unknown"


class CatalogApiClient:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def get_topic_owner_info(self, cluster_id: str, topic_name: str) -> tuple[str, str]:
        qualified_name = quote(f"{cluster_id}:{topic_name}", safe="")
        response = self.http.request_json(
            method="GET",
            path_or_url=f"/catalog/v1/entity/type/kafka_topic/name/{qualified_name}",
            allow_404=True,
        )
        if response is None:
            return UNKNOWN_OWNER, UNKNOWN_OWNER_EMAIL

        owner, owner_email = self._extract_owner_info(response)
        return owner or UNKNOWN_OWNER, owner_email or UNKNOWN_OWNER_EMAIL

    def get_topic_owners(self, cluster_id: str, topic_names: list[str]) -> tuple[dict[str, str], dict[str, str]]:
        owners: dict[str, str] = {}
        owner_emails: dict[str, str] = {}
        for topic_name in topic_names:
            owner, owner_email = self.get_topic_owner_info(cluster_id, topic_name)
            owners[topic_name] = owner
            owner_emails[topic_name] = owner_email
        return owners, owner_emails

    def _extract_owner_info(self, payload: Any) -> tuple[str | None, str | None]:
        attrs = self._get_path(payload, ("entity", "attributes"))
        if isinstance(attrs, dict):
            owner = self._stringify_owner(attrs.get("owner"))
            owner_email = attrs.get("ownerEmail")
            if isinstance(owner_email, str):
                owner_email = owner_email.strip() or None
            else:
                owner_email = None
            if owner:
                return owner, owner_email

        # Fallback: search for owner via legacy paths
        owner = self._extract_owner_legacy(payload)
        return owner, None

    def _extract_owner_legacy(self, payload: Any) -> str | None:
        candidate_paths = [
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
