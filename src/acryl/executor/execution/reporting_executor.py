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

from acryl.executor.execution.default_executor import DefaultExecutor, DefaultExecutorConfig
from acryl.executor.execution.task import TaskConfig
from acryl.executor.request.signal_request import SignalRequest
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.result.execution_result import ExecutionResult, Type
from acryl.executor.secret.secret_store import SecretStoreConfig
from datahub.metadata.schema_classes import ExecutionRequestResultClass, ExecutionRequestKeyClass, StructuredExecutionReportClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from typing import List, Optional
from pydantic import validator

DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME = "dataHubExecutionRequestResult"

class ReportingExecutorConfig(DefaultExecutorConfig):
    id: str = "default"
    task_configs: List[TaskConfig]
    secret_stores: List[SecretStoreConfig]

    graph_client: Optional[DataHubGraph] = None
    graph_client_config: Optional[DatahubClientConfig] = None

    class Config:
        arbitrary_types_allowed = True

    @validator('graph_client')
    def check_graph_connection(cls, v):
        if v is not None:
            v.test_connection()
        return v 

# Supports RUN_INGEST commands. 
class ReportingExecutor(DefaultExecutor):

    _datahub_graph: DataHubGraph
    _execution_timeout_sec: Optional[float] = 86400 # Default timeout is 1 day 

    def __init__(self, exec_config: ReportingExecutorConfig) -> None:
        super().__init__(exec_config)
        if exec_config.graph_client is not None:
            self._datahub_graph = exec_config.graph_client
        elif exec_config.graph_client_config is not None:
            self._datahub_graph = DataHubGraph(exec_config.graph_client_config)
        else:
            raise Exception("Invalid configuration provided. Missing DataHub graph client configs")
    
    # Run a list of tasks in sequence 
    def execute(self, request: ExecutionRequest) -> ExecutionResult:

        # Capture execution start time 
        start_time_ms = round(time.time() * 1000)

        # Build & emit an ACK mcp
        kickoff_mcp = self._build_kickoff_mcp(request, start_time_ms)
        self._datahub_graph.emit_mcp(kickoff_mcp)

        # Execute the request  
        request.progress_callback = lambda partial_report: self._datahub_graph.emit_mcp(self._build_progress_mcp(request, start_time_ms, partial_report=partial_report))
        exec_result = super().execute(request)

        # Execution has completed. Report the status. Only consider the first task in the list. 
        # TODO: Support timed out state. 
        status = exec_result.type.name

        # Capture the report
        report = exec_result.get_summary()

        # Get a structured report if available
        structured_report = exec_result.get_structured_report()

        # Compute execution duration (wall clock)
        end_time_ms = round(time.time() * 1000)
        duration_ms = end_time_ms - start_time_ms

        # Build & emit the completion mcp 
        completion_mcp = self._build_completion_mcp(exec_request=request, status=status, start_time_ms=start_time_ms, duration_ms=duration_ms, report=report, structured_report=structured_report)
        self._datahub_graph.emit_mcp(completion_mcp)

        return exec_result

    def signal(self, request: SignalRequest) -> None: 
        super().signal(request)
        # If the task cannot be found (is not actively executing), emit an empty cancellation event. 
        if request.signal == 'KILL':
            task_futures = self.task_futures.get(request.exec_id)
            if task_futures is None:
                # No task found. Simply emit a cancelled MCE. The start time is unclear. 
                # Build & emit the cancellation mcp 
                cancellation_mcp = self._build_empty_cancel_mcp(exec_id=request.exec_id)
                self._datahub_graph.emit_mcp(cancellation_mcp)
            
    # Builds an MCP to report the start of execution request handling 
    def _build_kickoff_mcp(self, exec_request: ExecutionRequest, start_time_ms: int) -> MetadataChangeProposalWrapper: 
        assert exec_request.exec_id, f"Missing exec_id for request {exec_request}"
        key_aspect = self._build_execution_request_key_aspect(exec_request.exec_id)
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.RUNNING.name,
            start_time_ms=start_time_ms
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect
        )

    def _build_progress_mcp(self, exec_request: ExecutionRequest, start_time_ms: int, partial_report: str) -> MetadataChangeProposalWrapper:
        assert exec_request.exec_id, f"Missing exec_id for request {exec_request}"
        key_aspect = self._build_execution_request_key_aspect(exec_request.exec_id)
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.RUNNING.name,
            start_time_ms=start_time_ms,
            report=partial_report,
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect
        )

    # Builds an MCP to report the completion of execution request handling 
    def _build_completion_mcp(self, exec_request: ExecutionRequest, status: str, start_time_ms: int, duration_ms: int, report: str, structured_report: Optional[str]) -> MetadataChangeProposalWrapper: 
        key_aspect = self._build_execution_request_key_aspect(exec_request.exec_id or "missing execution id")
        result_aspect = self._build_execution_request_result_aspect(
            status=status,
            start_time_ms=start_time_ms,
            duration_ms=duration_ms,
            report=report,
            structured_report=StructuredExecutionReportClass(type=exec_request.name, serializedValue=structured_report, contentType="application/json") if structured_report else None
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect
        )

    # Builds an MCP to report the completion of execution request handling 
    def _build_empty_cancel_mcp(self, exec_id: str) -> MetadataChangeProposalWrapper: 
        key_aspect = self._build_execution_request_key_aspect(exec_id)

        # TODO: Determine whether this is the "right" thing to do.
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.CANCELLED.name,
            start_time_ms=0, # TODO: Make start time optional
            duration_ms=None,
            report="No active execution request found."
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect
        )

    def _build_execution_request_key_aspect(self, execution_request_id: str) -> ExecutionRequestKeyClass:
        # Build the JSON map
        execution_request_key: ExecutionRequestKeyClass = ExecutionRequestKeyClass(id=execution_request_id)
        return execution_request_key

    def _build_execution_request_result_aspect(
        self, 
        status: str, 
        start_time_ms: int, 
        duration_ms: Optional[int] = None,
        report: Optional[str] = None,
        structured_report: Optional[StructuredExecutionReportClass] = None
        ) -> ExecutionRequestResultClass:

        execution_request_result: ExecutionRequestResultClass = ExecutionRequestResultClass(
            status=status,
            startTimeMs=start_time_ms,
            durationMs=duration_ms,
            report=report,
            structuredReport=structured_report
        )
        return execution_request_result

