"""The only way the collectors talk to Entur. One client = one ET-Client-Name."""
import uuid

import requests

from common import config


class EnturClient:
    def __init__(self, client_name: str):
        self.session = requests.Session()
        self.session.headers["ET-Client-Name"] = client_name

    def get_siri(self, url: str) -> bytes:
        """Full SIRI Lite snapshot as raw XML.
        A new requestorId on every call means Entur sends everything, not only changes."""
        params = {
            "datasetId": config.CODESPACE,
            "requestorId": str(uuid.uuid4()),
            "maxSize": config.SIRI_MAX_SIZE,
        }
        resp = self.session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def post_graphql(self, url: str, query: str) -> bytes:
        """GraphQL query, returns raw JSON. Raises if Entur answers with GraphQL errors."""
        resp = self.session.post(url, json={"query": query}, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        errors = resp.json().get("errors")
        if errors:
            raise RuntimeError(f"GraphQL errors: {errors}")
        return resp.content
