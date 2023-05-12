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

from acryl.executor.execution.task import Task
from acryl.executor.result.execution_result import Type
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.execution.task import TaskConfig
from acryl.executor.common.config import ConfigModel, BaseModel
from datahub.ingestion.run.pipeline import Pipeline

class InMemoryIngestionTaskConfig(ConfigModel):
    config_1: str

class InMemoryIngestionTaskArgs(BaseModel):
    recipe: dict

class InMemoryIngestionTask(Task):

    config: InMemoryIngestionTaskConfig

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        config = InMemoryIngestionTaskConfig.parse_obj(config)
        return cls(config)

    def __init__(self, config: InMemoryIngestionTaskConfig):
        self.config = config

    def execute(self, args: dict, ctx: ExecutionContext) -> None:
        validated_args = InMemoryIngestionTaskArgs.parse_obj(args)
        pipeline = Pipeline.create(
            validated_args.recipe
        )
        pipeline.run()
        pipeline.raise_from_status()

    def close(self) -> None:
        pass