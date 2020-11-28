# azutils

[![PythonVersion](https://img.shields.io/badge/python-3.6|3.7|3.8-blue.svg)](https://www.python.org/downloads/release/python-377/)
[![PiPY](https://img.shields.io/pypi/v/azutils.svg)](https://pypi.org/project/azutils/)


`azutils` is to provide convenient Python functions for Azure Services.

* Databricks,
* Azure Batch ?

## install

```bash
$ pip install azutils
```

## usage

* for `Databricks`

```python
from azutils.databricks import DatabricksClient
token = "..."

dc = DatabricksClient(token=token)

# list cluster
cluster_list = dc.clusters_list()

for cluster in cluster_list:
    print(cluster)

# * cluster_name: some-cluster
#   * cluster_id: XXXX-XXXXXX-xxxxxxx
#   * spark_version: 7.1.x-scala2.12
#   * driver_node_type: Standard_DS12_v2
#   * node_type: Standard_DS3_v2
#   * cost: 96.208 + 79.408 * NUM [YEN/HOUR]

cluster_id = "XXXX-XXXXXX-xxxxxxx"
print(dc.cluster_cost(cluster_id=cluster_id, start_time="2020-10-01", end_time="2020-10-31"))


# display cluster usage with `seaborn`
cluster_id_1 = "XXXX-XXXXXX-xxxxxx1"
cluster_id_2 = "XXXX-XXXXXX-xxxxxx2"
dc.cluster_running_time_as_sns(cluster_id=[cluster_id_1, cluster_id_2])

```