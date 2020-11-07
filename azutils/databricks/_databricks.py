from datetime import datetime, timezone, timedelta
from typing import List
from .utils import convert_datetime_to_milli_epoch


class Databricks:
    def __init__(self, payload):
        self.cluster_id = payload.get("cluster_id")
        self.driver = payload.get("driver")
        self.driver_node_type_id = payload.get("driver_node_type_id")
        self.executors = payload.get("executors")
        self.node_type_id = payload.get("node_type_id")
        self.spark_context_id = payload.get("spark_context_id")
        self.cluster_name = payload.get("cluster_name")
        self.spark_version = payload.get("spark_version")

    def __str__(self):
        s_list = [
            f"* cluster_name: {self.cluster_name}",
            f"  * cluster_id: {self.cluster_id}",
            f"  * spark_version: {self.spark_version}",
            f"  * driver_node_type: {self.driver_node_type_id}",
            f"  * node_type: {self.node_type_id}",

        ]
        return "\n".join(s_list)


class DatabricksEvents:
    """
    class for single DatabricksEvents

    See Also: https://docs.databricks.com/dev-tools/api/latest/clusters.html#events
    """

    def __init__(self, payload):
        self.cluster_id = payload.get("cluster_id")
        self.timestamp = payload.get("timestamp")
        self.dt = datetime.fromtimestamp(int(self.timestamp / 1000), timezone(timedelta(hours=9)))
        self.ymd = self.dt.strftime("%Y-%m-%d")
        self.hms = self.dt.strftime("%H:%M:%S")
        self.type = payload.get("type")
        self.details = payload.get("details")

    def __str__(self):
        return f"{self.ymd}T{self.hms}: {self.type}"


class DataBricksRunningTime:
    """
    DataBricks
    """

    def __init__(self, cluster_id, start_timestamp, end_timestamp, current_num_workers):
        self.cluster_id = cluster_id
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.current_num_workers = current_num_workers
        self.duration_sec = (self.end_timestamp - self.start_timestamp) / 1000
        self.start_dt = datetime.fromtimestamp(int(self.start_timestamp / 1000), timezone(timedelta(hours=9)))
        self.start_ymd = self.start_dt.strftime("%Y-%m-%d")
        self.start_hms = self.start_dt.strftime("%H:%M:%S")
        self.end_dt = datetime.fromtimestamp(int(self.end_timestamp / 1000), timezone(timedelta(hours=9)))
        self.end_ymd = self.end_dt.strftime("%Y-%m-%d")
        self.end_hms = self.end_dt.strftime("%H:%M:%S")

    @staticmethod
    def get_from_databricks_event(databricks_event: List[DatabricksEvents]):
        cluster_status = "TERMINATED"
        status_list = []
        start_timestamp = None
        current_num_workers = None
        for event in databricks_event[::-1]:

            if cluster_status == "TERMINATED" and event.type == "RUNNING":
                cluster_status = "RUNNING"
                start_timestamp = event.timestamp
                current_num_workers = event.details['current_num_workers']

            elif cluster_status == "RUNNING" and event.type == "UPSIZE_COMPLETED":
                cluster_id = event.cluster_id
                end_timestamp = event.timestamp
                current_num_workers = event.details['current_num_workers']
                status_list.append(DataBricksRunningTime(cluster_id, start_timestamp, end_timestamp, current_num_workers))
                start_timestamp = end_timestamp + 1

            elif cluster_status == "RUNNING" and event.type == "TERMINATING":
                cluster_id = event.cluster_id
                cluster_status = "TERMINATED"
                end_timestamp = event.timestamp
                status_list.append(DataBricksRunningTime(cluster_id, start_timestamp, end_timestamp, current_num_workers))
                start_timestamp = None
        return status_list

    def __str__(self):
        s_list = [
            f"* {self.duration_sec / 60: 0.1f}[min]",
            f"  * from: {self.start_ymd}T{self.start_hms}",
            f"  * to  : {self.end_ymd}T{self.end_hms}",
            f"  * cluster_num: {self.current_num_workers}"
        ]
        return "\n".join(s_list)


class DatabricksSetting:
    def __init__(self, start_timestamp, end_timestamp, databricks: Databricks):
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.databricks = databricks
        self.start_dt = datetime.fromtimestamp(int(self.start_timestamp / 1000), timezone(timedelta(hours=9)))
        self.start_ymd = self.start_dt.strftime("%Y-%m-%d")
        self.start_hms = self.start_dt.strftime("%H:%M:%S")
        self.end_dt = datetime.fromtimestamp(int(self.end_timestamp / 1000), timezone(timedelta(hours=9)))
        self.end_ymd = self.end_dt.strftime("%Y-%m-%d")
        self.end_hms = self.end_dt.strftime("%H:%M:%S")

    @staticmethod
    def get_from_databricks_event(databricks_event: List[DatabricksEvents]):
        status_list = []
        start_timestamp = None
        end_timestamp = None
        latest_databricks = None
        for event in databricks_event[::-1]:
            if event.type == "EDITED":
                if start_timestamp is None:
                    start_timestamp = event.timestamp
                else:
                    if end_timestamp is not None:
                        start_timestamp = end_timestamp
                end_timestamp = event.timestamp
                databricks_setting = DatabricksSetting(
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    databricks=Databricks(event.details['previous_attributes']))
                status_list.append(databricks_setting)

                latest_databricks = Databricks(event.details['attributes'])

        if latest_databricks is not None:
            # add latest Databricks
            databricks_setting = DatabricksSetting(
                start_timestamp=end_timestamp + 1000,
                end_timestamp=end_timestamp + 2000,
                databricks=latest_databricks)
            status_list.append(databricks_setting)
        return status_list

    def __str__(self):
        s_list = [
            f"* {self.start_ymd}T{self.start_hms} {self.end_ymd}T{self.end_hms}",
            f"  * driver_node_type: {self.databricks.driver_node_type_id}",
            f"  * node_type: {self.databricks.node_type_id}",
        ]
        return "\n".join(s_list)


class DatabricksSettingHistory:
    def __init__(self):
        self._databricks_setting_list: List[DatabricksSetting] = []

    def append(self, databricks_setting: DatabricksSetting):
        self._databricks_setting_list.append(databricks_setting)

    def extend(self, databricks_setting_list: List[DatabricksSetting]):
        self._databricks_setting_list.extend(databricks_setting_list)

    def get_at(self, timestamp) -> Databricks:
        """

        Args:
            timestamp: ISO8601 datetime format "%Y-%m-%d", "%Y-%m-%dT%H-%M-%S", or timestamp

        Returns:

        """
        timestamp = convert_datetime_to_milli_epoch(timestamp)
        initial_setting = self._databricks_setting_list[0]
        initial_cluster = initial_setting.databricks
        initial_timestamp = initial_setting.start_timestamp
        latest_cluster = initial_setting.databricks
        latest_timestamp = initial_setting.end_timestamp

        # get latest or initial cluster
        for cluster in self._databricks_setting_list[1:]:
            if initial_timestamp >= cluster.end_timestamp:
                initial_timestamp = cluster.start_timestamp
                initial_cluster = cluster.databricks
            elif latest_timestamp <= cluster.start_timestamp:
                latest_timestamp = cluster.end_timestamp
                latest_cluster = cluster.databricks
            #
            if cluster.start_timestamp <= timestamp <= cluster.end_timestamp:
                return cluster.databricks
        if timestamp <= initial_timestamp:
            return initial_cluster
        elif timestamp >= latest_timestamp:
            return latest_cluster
        else:
            raise ValueError("Out of range.")
