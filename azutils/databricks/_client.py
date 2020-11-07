from datetime import datetime
import requests
from typing import List, Optional, Union

from cachetools import cached
from ._databricks import (
    Databricks,
    DatabricksEvents,
    DataBricksRunningTime,
    DatabricksSetting,
    DatabricksSettingHistory
)


class DatabricksClient:
    """

    See Also: https://docs.databricks.com/dev-tools/api/latest/clusters.html
    """
    def __init__(self, token: str, region="japaneast"):
        self._token = token
        self._region = region
        self._base_url = f"https://{self._region}.azuredatabricks.net/api/2.0"
        self._headers = {"Authorization": f"Bearer {self._token}"}

    @cached(cache={})
    def clusters_list(self, raw=False) -> [dict, list]:
        url = f"{self._base_url}/clusters/list"

        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return [Databricks(cluster) for cluster in response.json()['clusters']]

    @staticmethod
    def _convert_datetime_to_milli_epoch(input_time):
        """
        Convert str or epoch-int to epoch-milli-int

        Args:
            input_time: ISO8601-format datetime, or epochtime

        Returns:

        """
        if type(input_time) is str:
            start_timestamp = int(datetime.fromisoformat(input_time).timestamp() * 1000)
            return start_timestamp
        elif type(input_time) is int:
            if len(str(input_time)) == 13:
                return input_time
            elif len(str(input_time)) == 10:
                return input_time * 1000

    def clusters_events(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500,
            raw=False) -> Union[List[dict], List[DatabricksEvents]]:

        start_timestamp = self._convert_datetime_to_milli_epoch(start_time)
        end_timestamp = self._convert_datetime_to_milli_epoch(end_time)
        result_list = []
        response = self._clusters_events(
            cluster_id=cluster_id,
            start_time=start_timestamp,
            end_time=end_timestamp
        )
        result_list.extend(response['events'])
        for _ in range(page_limit):
            if "next_page" not in response:
                break
            next_page = response['next_page']
            print(next_page)
            end_timestamp = next_page['end_time']
            offset = next_page['offset']
            response = self._clusters_events(
                cluster_id=cluster_id,
                start_time=start_timestamp,
                end_time=end_timestamp,
                offset=offset)
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

    def cluster_settings(self, cluster_id: str, page_limit: int = 500):
        cluster_events = self.clusters_events(cluster_id=cluster_id, page_limit=page_limit)
        return DatabricksSetting.get_from_databricks_event(cluster_events)

    def cluster_history(self, cluster_id: str, page_limit: int = 500) -> DatabricksSettingHistory:
        databricks_setting_history = DatabricksSettingHistory()
        databricks_setting_history.extend(self.cluster_settings(cluster_id=cluster_id, page_limit=page_limit))
        return databricks_setting_history
