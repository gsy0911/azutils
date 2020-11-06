import requests


class DatabricksClient:
    """

    See Also: https://docs.databricks.com/dev-tools/api/latest/clusters.html
    """
    def __init__(self, token: str, region="japaneast"):
        self._token = token
        self._region = region
        self._base_url = f"https://{self._region}.azuredatabricks.net/api/2.0"
        self._headers = {"Authorization": f"Bearer {self._token}"}

    def clusters_list(self) -> dict:
        url = f"{self._base_url}/clusters/list"

        res = requests.get(url, headers=self._headers)
        return res.json()

    def clusters_events(self, cluster_id: str, timestamp=None, offset=None, page_limit=500) -> dict:
        url = f"{self._base_url}/clusters/events"
        payload = {"cluster_id": cluster_id}
        res = requests.post(url, json=payload, headers=self._headers)
        return res.json()