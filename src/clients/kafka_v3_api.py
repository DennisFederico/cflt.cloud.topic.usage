from urllib.parse import quote

from http_client import HttpClient


class KafkaV3Client:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def list_topics_with_partitions(self, cluster_id: str, include_internal_topics: bool = False) -> dict[str, int]:
        topics: dict[str, int] = {}
        next_url: str | None = f"/kafka/v3/clusters/{cluster_id}/topics"

        while next_url:
            response = self.http.request_json(method="GET", path_or_url=next_url)
            for topic_data in response.get("data", []):
                topic_name = topic_data.get("topic_name")
                if not topic_name:
                    continue

                is_internal = bool(topic_data.get("is_internal", False)) or topic_name.startswith("_")
                if is_internal and not include_internal_topics:
                    continue

                partitions_count = topic_data.get("partitions_count")
                if isinstance(partitions_count, int):
                    topics[topic_name] = partitions_count
                else:
                    topics[topic_name] = self._count_topic_partitions(cluster_id, topic_name)

            next_url = (
                response.get("metadata", {}).get("next")
                or response.get("links", {}).get("next")
            )

        return topics

    def _count_topic_partitions(self, cluster_id: str, topic_name: str) -> int:
        encoded_topic = quote(topic_name, safe="")
        response = self.http.request_json(
            method="GET",
            path_or_url=f"/kafka/v3/clusters/{cluster_id}/topics/{encoded_topic}/partitions",
        )
        return len(response.get("data", []))
