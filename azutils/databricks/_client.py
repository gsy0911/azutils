import requests
from typing import List, Optional, Union

from cachetools import cached
import pandas as pd
import seaborn as sns

from ._databricks import (
    Databricks,
    DatabricksEvents,
    DataBricksRunningTime,
    DatabricksSetting,
    DatabricksSettingHistory,
    DatabricksJob,
    DatabricksView
)
from .utils import convert_datetime_to_milli_epoch

# set seaborn theme
sns.set(style='darkgrid')


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
    def jobs_runs_get(self, run_id: Union[int, str], raw=False) -> Union[dict, DatabricksJob]:
        """

        Args:
            run_id: run_id, ex: "5000" or 5000.
            raw: return response.json() or DatabricksJob-class

        Returns:
            dict or DatabricksJob

        See Also:
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
        """
        url = f"{self._base_url}/jobs/runs/get?run_id={run_id}"
        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return DatabricksJob(response.json())

    @cached(cache={})
    def clusters_get(self, cluster_id: str, raw=False) -> Union[dict, Databricks]:
        """

        Args:
            cluster_id:
            raw: return dict if True, Databricks-instance otherwise

        Returns:

        See Also:
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
        """
        url = f"{self._base_url}/clusters/get?cluster_id={cluster_id}"
        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return Databricks(response.json())

    @cached(cache={})
    def clusters_list(self, raw=False) -> [dict, List[Databricks]]:
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

    @cached(cache={})
    def jobs_runs_export(self, run_id: str, raw=False) -> [dict, List[DatabricksView]]:
        """

        Args:
            run_id:
            raw:

        Returns:

        See Also:
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-export
        """
        url = f"{self._base_url}/jobs/runs/export?run_id={run_id}"

        response = requests.get(url, headers=self._headers)
        if raw:
            return response.json()
        else:
            return DatabricksView.get_from_runs_export(response.json())

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
            return []
        result_list.extend(response['events'])
        for _ in range(page_limit):
            if "next_page" not in response:
                break
            next_page = response['next_page']
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
        """

        Args:
            cluster_id:
            start_time: ISO8601-format, i.e. %Y-%m-%dT%H:%M:%S, or %Y-%m-%d
            end_time: ISO8601-format, i.e. %Y-%m-%dT%H:%M:%S, or %Y-%m-%d
            offset:
            limit:

        Returns:

        See Also:
            https://docs.databricks.com/dev-tools/api/latest/clusters.html#events
        """
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

    def cluster_running_time_as_sns(
            self,
            cluster_id: Union[str, List[str]],
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500,
            **kwargs
    ):
        df = self.cluster_running_time_as_df(
            cluster_id=cluster_id,
            start_time=start_time,
            end_time=end_time,
            page_limit=page_limit
        )
        df['time'] = df.index
        data_df = df.pivot(index="time", columns="cluster_id", values="current_num_workers")
        return sns.lineplot(data=data_df, **kwargs)

    def cluster_running_time_as_df(
            self,
            cluster_id: Union[str, List[str]],
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500
    ) -> pd.DataFrame:
        if type(cluster_id) is str:
            running_time_list = self.cluster_running_time(
                cluster_id=cluster_id,
                start_time=start_time,
                end_time=end_time,
                page_limit=page_limit
            )
            data_list = [r.dumps() for r in running_time_list]

            df = pd.DataFrame(data_list)
            df.index = pd.to_datetime(df['start'])
            df = df.asfreq("T", method="bfill")
            return df
        elif type(cluster_id) is list:
            df_list = []
            for c in cluster_id:
                running_time_list = self.cluster_running_time(
                    cluster_id=c,
                    start_time=start_time,
                    end_time=end_time,
                    page_limit=page_limit
                )
                data_list = [r.dumps() for r in running_time_list]
                df = pd.DataFrame(data_list)
                df.index = pd.to_datetime(df['start'])
                df = df.asfreq("T", method="bfill")
                df_list.append(df)
            return pd.concat(df_list)
        raise ValueError("type of the `cluster_id` not matched")

    def cluster_running_time(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500) -> List[DataBricksRunningTime]:
        cluster_events = self.clusters_events(
            cluster_id=cluster_id, start_time=start_time, end_time=end_time, page_limit=page_limit)
        return DataBricksRunningTime.get_from_databricks_event(cluster_events)

    def cluster_settings(
            self,
            cluster_id: str,
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None,
            page_limit: int = 500) -> List[DatabricksSetting]:
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
            start_time: Optional[Union[int, str]] = None,
            end_time: Optional[Union[int, str]] = None) -> float:
        """

        Args:
            cluster_id: [required]
            start_time: ISO8601-format, i.e. %Y-%m-%dT%H:%M:%S, or %Y-%m-%d
            end_time: ISO8601-format, i.e. %Y-%m-%dT%H:%M:%S, or %Y-%m-%d

        Returns:

        """
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
            # get current cluster info
            if cluster is None:
                cluster = self.clusters_get(cluster_id=cluster_id)
            cost = r.duration_sec * (cluster.driver_node_cost() + cluster.node_cost() * r.current_num_workers) / 3600
            cost_list.append(cost)
        return sum(cost_list)
