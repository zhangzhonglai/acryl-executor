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

import logging
import traceback
import asyncio
import logging

from typing import Dict, List
from acryl.executor.common.config import ConfigModel
from acryl.executor.execution.executor import Executor
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.execution.task import Task, TaskConfig
from acryl.executor.execution.task_registry import TaskRegistry
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.result.execution_result import ExecutionResult, Type
from acryl.executor.request.signal_request import SignalRequest
from acryl.executor.secret.secret_store import SecretStore, SecretStoreConfig
from acryl.executor.secret.secret_store_registry import SecretStoreRegistry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

"""
Sets up the Task Executor 
"""
class DefaultExecutorConfig(ConfigModel):
    id: str
    task_configs: List[TaskConfig]
    secret_stores: List[SecretStoreConfig] = []

"""
Executes a Task Execution Request 
"""
class DefaultExecutor(Executor):
    id: str 

    # Tasks
    task_registry: TaskRegistry = TaskRegistry()
    task_instances: Dict[str, Task] = {}

    # Execution event loops + futures.
    task_event_loops: Dict[str, asyncio.AbstractEventLoop] = {}
    task_futures: Dict[str, List[asyncio.Task]] = {}

    # Secret Stores 
    secret_stores: List[SecretStore] = []

    def __init__(self, config: DefaultExecutorConfig) -> None:
        # Register the tasks 
        self.id = config.id
        self.secret_stores = self._create_secret_stores(config)
        for task_config in config.task_configs:
            self.register_task(task_config)

    def register_task(self, task: TaskConfig) -> None:
        # Store a registry of command -> task. 
        self.task_registry.register_lazy(task.name, task.type)

        # Create and initialize the task instances
        task_class = self.task_registry.get(task.name)
        try: 
            executor_context = ExecutorContext(self.id, self.secret_stores)
            task_instance = task_class.create(task.configs, executor_context)
            self.task_instances[task.name] = task_instance
        except Exception:
            raise Exception(f"Failed to create instance of task with name {task.name}: {traceback.format_exc(limit=3)}")

    # Run a list of tasks in sequence 
    def execute(self, request: ExecutionRequest) -> ExecutionResult:

        # 1. Create execution context
        executor_context = ExecutorContext(executor_id=self.id, secret_stores=self.secret_stores)

        # 2. Execute the task in the request 
        return self.execute_task(request, executor_context)

    def execute_task(self, request: ExecutionRequest, executor_context: ExecutorContext) -> ExecutionResult:
        # ASSUMPTION: Each time execute_task is called, it is called from a different thread.

        # 1. Build task execution context
        execution_context = ExecutionContext(request)
        execution_id = execution_context.get_execution_id()
        execution_report = execution_context.get_report()
        execution_report.report_info(f"Starting execution for task with name={request.name}")

        assert execution_id not in self.task_futures, "Already running task with same execution ID"

        # 2. Retrieve an instance of the task  
        task_instance = self.task_instances.get(request.name)
        execution_result = ExecutionResult(execution_context)
        execution_result.set_result_type(Type.FAILURE)
        if task_instance is None:
            execution_report.report_info(f"Failed to find task with name={request.name}")
            return execution_result 

        # 3. Execute task - task is expected to throw TaskError on failure. 
        try:
            # 3.1. Setup event loop for task execution.
            task_event_loop = asyncio.new_event_loop()
            self.task_event_loops[execution_id] = task_event_loop
            asyncio.set_event_loop(task_event_loop)

            # 3.2. Execute task via the thread's event loop. Store the executing task such that it can be cancelled. 
            # TODO: Have an async version of this method. 
            task_future = task_event_loop.create_task(task_instance.execute(request.args, execution_context))
            task_future.set_name(f"dispatch-{execution_id}")
            logger.debug(f"Task for {execution_id} created")
            self.task_futures[execution_id] = [task_future]
            task_event_loop.run_until_complete(task_future)
            logger.debug(f"Task for {execution_id} completed")
        except Exception:
            execution_report.report_info(f"Caught exception EXECUTING task_id={execution_context.get_execution_id()}, name={request.name}, stacktrace={traceback.format_exc(limit=20)}")
            return execution_result 
        except asyncio.exceptions.CancelledError:
            execution_report.report_info(f"Execution cancelled while EXECUTING task_id={execution_context.get_execution_id()}, name={request.name}, stacktrace={traceback.format_exc(limit=3)}")
            execution_result.set_result_type(Type.CANCELLED)
            return execution_result 
        finally:
            # task_event_loop.close()
            # del self.task_futures[execution_id]
            # del self.task_event_loops[execution_id]

            logger.debug(f"Cleaned up task for {execution_id}")

        execution_report.report_info(f"Finished execution for task with name={request.name}")
        execution_result.set_result_type(Type.SUCCESS)
        return execution_result

    def signal(self, request: SignalRequest) -> None: 
        if request.signal == 'KILL':
            task_futures = self.task_futures.get(request.exec_id)
            if task_futures is None:
                logger.error(f"Received KILL for missing task ID {request.exec_id}")
            else:
                event_loop = self.task_event_loops[request.exec_id]
                for task_future in task_futures:
                    # Cancel the task if not complete. 
                    if not task_future.done():
                        logger.debug(f"Trying to cancel {task_future}")
                        event_loop.call_soon_threadsafe(task_future.cancel)

    def shutdown(self) -> None:
        for task_name, task_instance in self.task_instances.items():
            try:
                task_instance.close()
            except Exception:
                logger.warn(f"Failed to shutdown task with name {task_name}")

    def get_id(self) -> str:
        return self.id
    
    def _create_secret_stores(self, config: DefaultExecutorConfig) -> List[SecretStore]:
        secret_stores = [] 
        for secret_store_config in config.secret_stores:
            secret_stores.append(self._create_secret_store(secret_store_config))
        return secret_stores

    def _create_secret_store(self, config: SecretStoreConfig) -> SecretStore:
        # Create a secret store registry 
        secret_store_registry: SecretStoreRegistry = SecretStoreRegistry()

        # Get the secret store configs
        secret_store_type: str = config.type
        secret_store_configs: dict = config.config 

        # Fetch the correct secret store, or register a new one
        if not secret_store_registry.is_enabled(secret_store_type):
            # Custom Secret Store found. Register it. 
            secret_store_registry.register_lazy(secret_store_type, secret_store_type)
        
        # Instantiate the secret store class
        secret_store_class = secret_store_registry.get(secret_store_type)

        # Create & return new instance of secret store 
        return secret_store_class.create(config.config)
