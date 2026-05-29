from typing import Any
from urllib.parse import urlencode
from urllib.parse import quote

from http_client import HttpClient


UNKNOWN_OWNER = "unknown"
UNKNOWN_OWNER_EMAIL = ""


class CatalogApiClient:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def list_topics_metadata(
        self,
        cluster_id: str,
        include_internal_topics: bool = True,
        page_limit: int = 500,
    ) -> tuple[dict[str, int], dict[str, str], dict[str, str]]:
        if page_limit < 1:
            raise ValueError("page_limit must be >= 1")

        topic_partitions: dict[str, int] = {}
        owners: dict[str, str] = {}
        owner_emails: dict[str, str] = {}
        offset = 0
        qualified_name_prefix = cluster_id

        while True:
            query = urlencode(
                [
                    ("type", "kafka_topic"),
                    ("attrName", "qualifiedName"),
                    ("attrValuePrefix", qualified_name_prefix),
                    ("attr", "partitionsCount"),
                    ("attr", "owner"),
                    ("attr", "ownerEmail"),
                    ("deleted", "false"),
                    ("limit", str(page_limit)),
                    ("offset", str(offset)),
                ]
            )
            response = self.http.request_json(
                method="GET",
                path_or_url=f"/catalog/v1/search/attribute?{query}",
            )

            entities = response.get("entities", [])
            if not isinstance(entities, list) or not entities:
                break

            for entity in entities:
                topic_name, partitions, owner, owner_email = self._extract_topic_metadata_entry(entity, cluster_id)
                if not topic_name:
                    continue

                is_internal = topic_name.startswith("_")
                if is_internal and not include_internal_topics:
                    continue

                topic_partitions[topic_name] = partitions
                owners[topic_name] = owner or UNKNOWN_OWNER
                owner_emails[topic_name] = owner_email or UNKNOWN_OWNER_EMAIL

            if len(entities) < page_limit:
                break
            offset += page_limit

        return topic_partitions, owners, owner_emails

    def list_topics_with_partitions(
        self,
        cluster_id: str,
        include_internal_topics: bool = True,
        page_limit: int = 500,
    ) -> dict[str, int]:
        topic_partitions, _, _ = self.list_topics_metadata(
            cluster_id=cluster_id,
            include_internal_topics=include_internal_topics,
            page_limit=page_limit,
        )
        return topic_partitions

    def topic_exists(self, cluster_id: str, topic_name: str) -> bool:
        qualified_name = quote(f"{cluster_id}:{topic_name}", safe="")
        response = self.http.request_json(
            method="GET",
            path_or_url=f"/catalog/v1/entity/type/kafka_topic/name/{qualified_name}",
            allow_404=True,
        )
        return response is not None

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

    def update_topic_owner(self, cluster_id: str, topic_name: str, owner: str, owner_email: str) -> None:
        qualified_name = f"{cluster_id}:{topic_name}"
        payload = {
            "entity": {
                "typeName": "kafka_topic",
                "attributes": {
                    "qualifiedName": qualified_name,
                    "owner": owner,
                    "ownerEmail": owner_email,
                },
            }
        }
        self.http.request_json(
            method="PUT",
            path_or_url="/catalog/v1/entity",
            json_body=payload,
        )

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

    def _extract_topic_metadata_entry(self, payload: Any, cluster_id: str) -> tuple[str | None, int, str | None, str | None]:
        attrs = self._get_path(payload, ("entity", "attributes"))
        if not isinstance(attrs, dict):
            attrs = self._get_path(payload, ("attributes",))
        if not isinstance(attrs, dict):
            attrs = payload if isinstance(payload, dict) else {}

        topic_name = self._extract_topic_name(attrs, cluster_id)
        partitions = self._coerce_int(attrs.get("partitionsCount"), default=0)
        owner = self._stringify_owner(attrs.get("owner"))
        owner_email = attrs.get("ownerEmail")
        if isinstance(owner_email, str):
            owner_email = owner_email.strip() or None
        else:
            owner_email = None
        return topic_name, partitions, owner, owner_email

    def _extract_topic_name(self, attrs: dict[str, Any], cluster_id: str) -> str | None:
        name = attrs.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()

        qualified_name = attrs.get("qualifiedName")
        if isinstance(qualified_name, str) and qualified_name.strip():
            return self._topic_name_from_qualified_name(qualified_name.strip(), cluster_id)

        return None

    @staticmethod
    def _topic_name_from_qualified_name(qualified_name: str, cluster_id: str) -> str | None:
        marker = f":{cluster_id}:"
        index = qualified_name.find(marker)
        if index >= 0:
            topic_name = qualified_name[index + len(marker):].strip()
            return topic_name or None

        prefix = f"{cluster_id}:"
        if qualified_name.startswith(prefix):
            topic_name = qualified_name[len(prefix):].strip()
            return topic_name or None

        if ":" in qualified_name:
            topic_name = qualified_name.rsplit(":", maxsplit=1)[-1].strip()
            return topic_name or None

        return qualified_name.strip() or None

    @staticmethod
    def _coerce_int(value: Any, default: int) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return default
        return default

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
