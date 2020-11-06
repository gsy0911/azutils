from datetime import datetime, timezone, timedelta


class Databricks:
    def __init__(self, payload):
        data = payload.copy()
        self.cluster_id = data.get("cluster_id")
        self.driver = data.get("driver")
        self.driver_node_type_id = data.get("driver_node_type_id")
        self.executors = data.get("executors")
        self.node_type_id = data.get("node_type_id")
        self.spark_context_id = data.get("spark_context_id")
        self.cluster_name = data.get("cluster_name")
        self.spark_version = data.get("spark_version")

    def __str__(self):
        s_list = [
            f"# cluster_name: {self.cluster_name}",
            f"\t * cluster_id: {self.cluster_id}",
            f"\t * spark_version: {self.spark_version}",
            f"\t * driver_node_type: {self.driver_node_type_id}",
            f"\t * node_type: {self.node_type_id}",

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
