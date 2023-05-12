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

import sys
import subprocess
import os
import logging
import asyncio
from collections import deque
from acryl.executor.execution.task import Task
from acryl.executor.execution.task import TaskError
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.common.config import ConfigModel, PermissiveConfigModel
from acryl.executor.execution.sub_process_task_common import SubProcessTaskUtil
from typing import List

logger = logging.getLogger(__name__)

class SubProcessTestConnectionTaskConfig(ConfigModel):
    tmp_dir: str = "/tmp/datahub/ingest"

class SubProcessTestConnectionTaskArgs(PermissiveConfigModel):
    recipe: str
    version: str = "latest"

class SubProcessTestConnectionTask(Task):

    config: SubProcessTestConnectionTaskConfig
    tmp_dir: str # Location where tmp files will be written (recipes) 
    ctx: ExecutorContext

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        config_parsed = SubProcessTestConnectionTaskConfig.parse_obj(config)
        return cls(config_parsed, ctx)

    def __init__(self, config: SubProcessTestConnectionTaskConfig, ctx: ExecutorContext):
        self.config = config
        self.tmp_dir = config.tmp_dir
        self.ctx = ctx 

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:

        exec_id = ctx.exec_id # The unique execution id.

        exec_out_dir = f"{self.tmp_dir}/{exec_id}"

        # 0. Validate arguments 
        validated_args = SubProcessTestConnectionTaskArgs.parse_obj(args)

        # 1. Resolve the recipe (combine it with others)
        recipe: dict = SubProcessTaskUtil._resolve_recipe(validated_args.recipe, execution_ctx=ctx, executor_ctx=self.ctx)

        # 2. Write recipe file to local FS (requires write permissions to /tmp directory)
        recipe_file: str = "recipe.yml"  
        SubProcessTaskUtil._write_recipe_to_file(exec_out_dir, recipe_file, recipe)
        recipe_file_loc: str = f"{exec_out_dir}/{recipe_file}"

        # 3. Spin off subprocess to run the run_ingest.sh script
        datahub_version = validated_args.version # The version of DataHub CLI to use. 
        plugins = recipe["source"]["type"] # The source type -- ASSUMPTION ALERT: This should always correspond to the plugin name. 
        command_script: str = "run_test_connection.sh" # TODO: Make sure this is EXECUTABLE.  
        report_out_file: str = f"{exec_out_dir}/connection_report.json"
        stdout_lines: deque = deque(maxlen=SubProcessTaskUtil.MAX_LOG_LINES)

        # TODO: Inject a token into the recipe to run such that it "just works" (currently, a token is required)

        ingest_process = subprocess.Popen([command_script, exec_id, datahub_version, plugins, self.tmp_dir, recipe_file_loc, report_out_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        try: 
            while ingest_process.poll() is None:
                assert ingest_process.stdout
                line = ingest_process.stdout.readline()

                sys.stdout.write(line)
                stdout_lines.append(line)
                await asyncio.sleep(0)

            return_code = ingest_process.poll()

        except asyncio.CancelledError:
            # Terminate the running child process 
            ingest_process.terminate()
            raise  

        finally:
            if os.path.exists(report_out_file):
                with open(report_out_file,'r') as structured_report_fp:
                    ctx.get_report().set_structured_report(structured_report_fp.read())

            ctx.get_report().set_logs(SubProcessTaskUtil._format_log_lines(stdout_lines))
            
            # Cleanup by removing the exec out directory
            SubProcessTaskUtil._remove_directory(exec_out_dir)

        if return_code != 0:
            # Failed
            ctx.get_report().report_info("Failed to execute 'datahub test connection'")
            raise TaskError("Failed to execute 'datahub test connection'") 
        
        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub test connection'")


    def close(self) -> None:
        pass