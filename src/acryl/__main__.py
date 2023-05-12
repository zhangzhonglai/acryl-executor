# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import json
import os
from unittest import mock


from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.request.signal_request import SignalRequest
from acryl.executor.execution.task import TaskConfig
from acryl.executor.dispatcher.default_dispatcher import DefaultDispatcher
from acryl.executor.execution.reporting_executor import ReportingExecutor, ReportingExecutorConfig
from acryl.executor.secret.datahub_secret_store import DataHubSecretStoreConfig
from acryl.executor.secret.secret_store import SecretStoreConfig
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.ingestion.api.sink import NoopWriteCallback
# this is an integration test of the functionality, basically, manually verified  
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.common import RecordEnvelope
   

class FakeDataHubGraph(DataHubGraph):
    def __init__(self, file_name: str = "mcp_logs.json"):
        self.file_name = file_name
        self.file_sink = FileSink(PipelineContext(run_id="test"), FileSinkConfig(filename=file_name))

    def test_connection(self) -> dict:
        return {"noCode": True}

    def emit_mcp(self, mcp):
        self.file_sink.write_record_async(RecordEnvelope(record=mcp, metadata={}), write_callback=NoopWriteCallback())

    def close(self):
        self.file_sink.close()

def setup():
    # Main function

    env_map = {"HOST_PORT": "http://localhost:8080",
                "DATABASE": "datahub",
                "MYSQL_USERNAME": "datahub",
                "MYSQL_PASSWORD": "datahub",
            }
    for k,v in env_map.items():
        os.environ[k] = v

    fake_client = FakeDataHubGraph()
    # Build and register executors with the dispatcher
    local_task_config = TaskConfig(
        name="RUN_INGEST",
        type="acryl.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask", 
        configs=dict({}))
    local_test_connection_task_config = TaskConfig(
        name="TEST_CONNECTION",
        type="acryl.executor.execution.sub_process_test_connection_task.SubProcessTestConnectionTask",
        configs=dict({})
    )
    local_exec_config = ReportingExecutorConfig(
        id="default", 
        task_configs=[local_task_config, local_test_connection_task_config],
        secret_stores=[
            SecretStoreConfig(type="env", config=dict({})),
            SecretStoreConfig(type="datahub", config=DataHubSecretStoreConfig(
                graph_client_config=DatahubClientConfig(
                    server="http://localhost:8080",
                    token="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY0MjIyMzE5MywianRpIjoiZjIwZjRmMmQtMThlNS00NTRmLTgyMzQtMTg1YzIxZjAwN2QzIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.e3N4_Z_3QhviYmLW8UXS2KhhcrBIMinDhSjnR05bMbE"
                )
            ))
        ],
        graph_client_config=DatahubClientConfig(
            server="http://localhost:8080",
            token="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY0MjIyMzE5MywianRpIjoiZjIwZjRmMmQtMThlNS00NTRmLTgyMzQtMTg1YzIxZjAwN2QzIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.e3N4_Z_3QhviYmLW8UXS2KhhcrBIMinDhSjnR05bMbE"
        ),
        graph_client=fake_client
    )
    return (local_task_config, local_exec_config, fake_client, local_test_connection_task_config)


if __name__ == "__main__":

    (local_task_config, local_exec_config, fake_client, local_test_connection_task_config) = setup()
    local_executor = ReportingExecutor(local_exec_config)
    dispatcher = DefaultDispatcher([local_executor])

    endpoint = "http://localhost:8080/" # get from config
    recipe_dictionary = {
        "source": {
            "type": "mysql",
            "config": {
                "filename": "foobar.yaml"
                }
            },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server":"http://localhost:8080", 
                "token":"eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY0MjIyMzE5MywianRpIjoiZjIwZjRmMmQtMThlNS00NTRmLTgyMzQtMTg1YzIxZjAwN2QzIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.e3N4_Z_3QhviYmLW8UXS2KhhcrBIMinDhSjnR05bMbE"
                }
        }
    }
    args = {'version': "0.8.41.1rc2",
            'recipe': json.dumps(recipe_dictionary)
    }

    exec_request = ExecutionRequest(exec_id="1234", name="TEST_CONNECTION", args=args)
    start_time = time.time()
    dispatcher.dispatch(exec_request)
    
    dispatcher.shutdown()
    fake_client.close()
    # Now kill the process. 
    # signal_request = SignalRequest(exec_id="1234", signal="KILL")
    # dispatcher.dispatch_signal(signal_request)

    


