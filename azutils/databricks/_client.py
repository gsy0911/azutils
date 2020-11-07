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
from .utils import convert_datetime_to_milli_epoch


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
        """

        Args:
            raw:

        Returns:

        See Also:
            https://docs.databricks.com/dev-tools/api/latest/clusters.html#list
        """
        url = f"{self._base_url}/clusters/list"

        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return [Databricks(cluster) for cluster in response.json()['clusters']]

    def clusters_events(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500,
            raw=False) -> Union[List[dict], List[DatabricksEvents]]:

        start_timestamp = convert_datetime_to_milli_epoch(start_time)
        end_timestamp = convert_datetime_to_milli_epoch(end_time)
        result_list = []
        response = self._clusters_events(
            cluster_id=cluster_id,
            start_time=start_timestamp,
            end_time=end_timestamp
        )
        if "events" not in response:
            print(response)
            return []
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

    @cached(cache={})
    def _clusters_events(
            self,
            cluster_id: str,
            start_time: Optional[int] = None,
            end_time: Optional[int] = None,
            offset: Optional[int] = None,
            limit=500) -> dict:
        url = f"{self._base_url}/clusters/events"
        payload = {
            "cluster_id": cluster_id,
            "limit": limit
        }
        if start_time is not None:
            payload['start_time'] = start_time
        if end_time is not None:
            payload['end_time'] = end_time
        if offset is not None:
            payload['offset'] = offset
        response = requests.post(url, json=payload, headers=self._headers)
        data = response.json()
        return data

    def cluster_running_time(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500):
        cluster_events = self.clusters_events(
            cluster_id=cluster_id, start_time=start_time, end_time=end_time, page_limit=page_limit)
        return DataBricksRunningTime.get_from_databricks_event(cluster_events)

    def cluster_settings(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500):
        cluster_events = self.clusters_events(
            cluster_id=cluster_id, start_time=start_time, end_time=end_time, page_limit=page_limit)
        return DatabricksSetting.get_from_databricks_event(cluster_events)

    def cluster_history(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500) -> DatabricksSettingHistory:
        databricks_setting_history = DatabricksSettingHistory()
        databricks_setting_history.extend(self.cluster_settings(
            cluster_id=cluster_id, start_time=start_time, end_time=end_time, page_limit=page_limit))
        return databricks_setting_history

    def cluster_cost(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]],
            end_time: Optional[Union[int, str]]):
        payload = {
            "cluster_id": cluster_id,
            "start_time": start_time,
            "end_time": end_time
        }
        running_time_list = self.cluster_running_time(**payload)
        cluster_history = self.cluster_history(**payload)

        cost_list = []
        for r in running_time_list:
            timestamp = r.start_timestamp
            cluster = cluster_history.get_at(timestamp)
            if cluster is None:
                continue
            cost = r.duration_sec * (cluster.driver_node_cost() + cluster.node_cost() * r.current_num_workers) / 3600
            cost_list.append(cost)
        return sum(cost_list)
