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

import contextlib
import json
from enum import Enum
from acryl.executor.context.execution_context import ExecutionContext
from typing import Optional


class Type(Enum):
    SUCCESS = 1
    FAILURE = 2
    TIMEOUT = 3
    CANCELLED = 4
    RUNNING = 5


class ExecutionResult:

    # Result type, success or failure
    type: Type

    # Execution context
    context: ExecutionContext

    def __init__(self, ctx: ExecutionContext) -> None:
        self.context = ctx

    def get_context(self) -> ExecutionContext:
        return self.context

    def set_result_type(self, type: Type) -> None:
        self.type = type

    def get_result_type(self) -> Type:
        return self.type

    def get_summary(self) -> str:
        summary = (
            f"~~~~ Execution Summary - {self.get_context().get_task_name()} ~~~~\n"
        )

        if self.type == Type.FAILURE:
            summary += "Execution finished with errors."
        elif self.type == Type.CANCELLED:
            summary += "Execution was cancelled by request."
        else:
            summary += "Execution finished successfully!"
        summary += "\n"

        summary += f"{self.get_context().get_report().as_string()}\n"
        summary += "\n"

        structured_report = self.get_structured_report()
        if structured_report is not None:
            summary += f"~~~~ Ingestion Report ~~~~\n"
            with contextlib.suppress(Exception):
                # Try to reformat the structured report.
                structured_report = json.dumps(json.loads(structured_report), indent=2)
            summary += f"{structured_report}\n"
            summary += "\n"

        summary += f"~~~~ Ingestion Logs ~~~~\n"
        summary += f"{self.get_context().get_report().get_logs()}\n"
        return summary

    def get_structured_report(self) -> Optional[str]:
        return self.get_context().get_report().get_structured_report()

    def pretty_print_summary(self):
        print(self.get_summary())
