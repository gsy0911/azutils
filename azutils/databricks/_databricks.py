from datetime import datetime, timezone, timedelta
from typing import List, Optional
from .utils import convert_datetime_to_milli_epoch


class DatabricksCost:
    def __init__(self, workload: str, level: str, instance: str, vm_price: float, dbu_price: float):
        self.workload = workload
        self._level = level
        self._instance_id = instance
        self._vm_price = vm_price
        self._dbu_price = dbu_price
        self.node_type_id = "_".join([level, instance])
        self.total_price = vm_price + dbu_price

    def __str__(self):
        s_list = [
            f"* node_type_id: {self.node_type_id}",
            f"  * vm_price: {self._vm_price}",
            f"  * dbu_price: {self._dbu_price}"
        ]
        return "\n".join(s_list)


class DatabricksCostReferences:
    """

    See Also:
        https://azure.microsoft.com/ja-jp/pricing/details/databricks/
    """
    # workload constant
    ALL_PURPOSE_COMPUTE = "UI"
    JOBS_COMPUTE = "JOB"
    # level constant
    PREMIUM = "Premium"
    STANDARD = "Standard"
    COST_LIST = [
        DatabricksCost(ALL_PURPOSE_COMPUTE, STANDARD, "DS3_v2", 45.808, 33.60)
    ]

    @classmethod
    def list(cls):
        for cost in cls.COST_LIST:
            print(cost)

    @classmethod
    def get(
            cls,
            *,
            node_type_id: Optional[str] = None,
            workload: Optional[str] = None,
            level: Optional[str] = None,
            instance: Optional[str] = None) -> Optional[DatabricksCost]:
        if node_type_id is not None:
            _node_type_id = node_type_id
        else:
            _node_type_id = "_".join([level, instance])

        if workload is not None:
            _workload = workload
        else:
            _workload = cls.ALL_PURPOSE_COMPUTE

        for cost in cls.COST_LIST:
            if cost.node_type_id == _node_type_id and cost.workload == _workload:
                return cost
        return None


class Databricks:
    COST_INFO = {
            'Standard_DS3_v2': 79.408,
            'Standard_DS4_v2': 158.816,
            'Standard_D8s_v3': 124.992,
            'Standard_DS5_v2': 317.632,
            'Standard_D32s_v3': 499.968,
            'Standard_D16s_v3': 249.984,
            'Standard_D16_v3': 249.984,
            'Standard_D32_v3': 499.968,
            'Standard_DS12_v2': 96.208,
            'Standard_D64s_v3': 999.936,
            'Standard_F8s': 137.536,
            'Standard_F4s': 68.768
        }

    def __init__(self, payload):
        self.cluster_id = payload.get("cluster_id")
        self.driver = payload.get("driver")
        self.driver_node_type_id = payload.get("driver_node_type_id")
        self.executors = payload.get("executors")
        self.node_type_id = payload.get("node_type_id")
        self.spark_context_id = payload.get("spark_context_id")
        self.cluster_name = payload.get("cluster_name")
        self.spark_version = payload.get("spark_version")

    def driver_node_cost(self):
        candidate_cost = self.COST_INFO.get(self.driver_node_type_id, 0)
        return candidate_cost

    def node_cost(self):
        candidate_cost = self.COST_INFO.get(self.node_type_id, 0)
        return candidate_cost

    def __str__(self):
        s_list = [
            f"* cluster_name: {self.cluster_name}",
            f"  * cluster_id: {self.cluster_id}",
            f"  * spark_version: {self.spark_version}",
            f"  * driver_node_type: {self.driver_node_type_id}",
            f"  * node_type: {self.node_type_id}",
            f"  * cost: {self.driver_node_cost():.3f} + {self.node_cost():.3f} * NUM [YEN/HOUR]"
        ]
        return "\n".join(s_list)


class DatabricksJob:
    def __init__(self, payload: dict):
        self.job_id = payload.get("job_id")
        self.run_id = payload.get("run_id")
        self.run_name = payload.get("run_name")
        self.run_page_url = payload.get("run_page_url")
        self.run_type = payload.get("run_type")
        self.number_in_job = payload.get("number_in_job")
        self.state = payload.get("state")
        payload_for_databricks = payload['cluster_spec']['new_cluster']
        payload_for_databricks.update(payload['cluster_instance'])
        payload_for_databricks.update(
            {
                "driver_node_type_id": payload['cluster_spec']['new_cluster']['node_type_id'],
                "cluster_name": payload.get("run_name")
            }
        )
        self.databricks = Databricks(payload_for_databricks)
        self.libraries = payload.get("libraries")
        self.start_time = payload.get("start_time")
        self.setup_duration = payload.get("setup_duration")
        self.execution_duration = payload.get("execution_duration")

    def __str__(self):
        s_list = [
            str(self.databricks),
            f"  * job_id: {self.job_id}",
            f"  * run_id: {self.run_id}",
            f"  * start_at: {self.start_time}",
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
                running_time = DataBricksRunningTime(cluster_id, start_timestamp, end_timestamp, current_num_workers)
                status_list.append(running_time)
                start_timestamp = end_timestamp + 1

            elif cluster_status == "RUNNING" and event.type == "TERMINATING":
                cluster_id = event.cluster_id
                cluster_status = "TERMINATED"
                end_timestamp = event.timestamp
                running_time = DataBricksRunningTime(cluster_id, start_timestamp, end_timestamp, current_num_workers)
                status_list.append(running_time)
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
    """
    Keep cluster-setting from start_timestamp to end_timestamp.
    """
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

    def get_at(self, timestamp) -> Optional[Databricks]:
        """

        Args:
            timestamp: ISO8601 datetime format "%Y-%m-%d", "%Y-%m-%dT%H-%M-%S", or timestamp

        Returns:

        """
        timestamp = convert_datetime_to_milli_epoch(timestamp)
        if len(self._databricks_setting_list) == 0:
            return None
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
