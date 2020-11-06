import requests
from typing import List, Optional, Union
from ._databricks import Databricks, DatabricksEvents, DataBricksRunningTime


class DatabricksClient:
    """

    See Also: https://docs.databricks.com/dev-tools/api/latest/clusters.html
    """
    def __init__(self, token: str, region="japaneast"):
        self._token = token
        self._region = region
        self._base_url = f"https://{self._region}.azuredatabricks.net/api/2.0"
        self._headers = {"Authorization": f"Bearer {self._token}"}

    def clusters_list(self, raw=False) -> [dict, list]:
        url = f"{self._base_url}/clusters/list"

        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return [Databricks(cluster) for cluster in response.json()['clusters']]

    def clusters_events(
            self,
            cluster_id: str,
            page_limit: int = 500,
            raw=False) -> Union[List[dict], List[DatabricksEvents]]:
        result_list = []
        response = self._clusters_events(cluster_id=cluster_id)
        result_list.extend(response['events'])
        for _ in range(page_limit):
            next_page = response['next_page']
            print(next_page)
            end_time = next_page['end_time']
            offset = next_page['offset']
            response = self._clusters_events(cluster_id=cluster_id, end_time=end_time, offset=offset)
            result_list.extend(response['events'])
        if raw:
            return result_list
        else:
            return [DatabricksEvents(event) for event in result_list]

    def _clusters_events(
            self,
            cluster_id: str,
            end_time: Optional[int] = None,
            offset: Optional[int] = None) -> dict:
        url = f"{self._base_url}/clusters/events"
        payload = {"cluster_id": cluster_id}
        if end_time is not None:
            payload['end_time'] = end_time
        if offset is not None:
            payload['offset'] = offset
        response = requests.post(url, json=payload, headers=self._headers)
        return response.json()

    def cluster_running_time(self, cluster_id: str, page_limit: int = 500):
        cluster_events = self.clusters_events(cluster_id=cluster_id, page_limit=page_limit)
        return DataBricksRunningTime.get_from_databricks_event(cluster_events)
