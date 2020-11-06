

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
