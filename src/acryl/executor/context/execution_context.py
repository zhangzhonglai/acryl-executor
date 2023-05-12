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

import uuid
from acryl.executor.report.execution_report import ExecutionReport
from acryl.executor.request.execution_request import ExecutionRequest

def generate_execution_id() -> str:
    # return new execution id 
    return str(uuid.uuid4())

"""
Context object passed down into the Task being executed, when its executed 
"""
class ExecutionContext:

    # Execution id 
    exec_id: str

    # The original request
    request: ExecutionRequest

    # The report associated with the task 
    report: ExecutionReport

    def __init__(self, request: ExecutionRequest) -> None:
        self.request = request
        if request.exec_id is None:
            self.exec_id = generate_execution_id()
        else:
            self.exec_id = request.exec_id
        self.report = ExecutionReport(self.exec_id)

    def get_execution_id(self) -> str:
        return self.exec_id

    def get_report(self) -> ExecutionReport:
        return self.report

    def get_task_name(self) -> str:
        return self.request.name