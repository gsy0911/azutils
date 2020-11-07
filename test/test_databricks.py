from azutils.databricks import DatabricksClient


def test_client():
    _ = DatabricksClient(token="example_token")
