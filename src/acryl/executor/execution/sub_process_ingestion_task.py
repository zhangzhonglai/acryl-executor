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

from asyncio import tasks
from datetime import datetime, timedelta
import os
import sys
import logging
import asyncio
from collections import deque
from typing import Optional
from acryl.executor.execution.sub_process_task_common import SubProcessTaskUtil
from acryl.executor.execution.task import Task
from acryl.executor.execution.task import TaskError
from acryl.executor.result.execution_result import Type
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.common.config import ConfigModel, PermissiveConfigModel

logger = logging.getLogger(__name__)

class SubProcessIngestionTaskConfig(ConfigModel):
    tmp_dir: str = "/tmp/datahub/ingest"
    log_dir: str = "/tmp/datahub/logs"
    heartbeat_time_seconds: int = 2
    max_log_lines: int = SubProcessTaskUtil.MAX_LOG_LINES

class SubProcessIngestionTaskArgs(PermissiveConfigModel):
    recipe: str
    version: str = "latest"
    debug_mode: str = "false" # Expected values are "true" or "false". 

class SubProcessIngestionTask(Task):

    config: SubProcessIngestionTaskConfig
    tmp_dir: str # Location where tmp files will be written (recipes) 
    ctx: ExecutorContext

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        return cls(SubProcessIngestionTaskConfig.parse_obj(config), ctx)

    def __init__(self, config: SubProcessIngestionTaskConfig, ctx: ExecutorContext):
        self.config = config
        self.tmp_dir = config.tmp_dir
        self.ctx = ctx 

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:

        exec_id = ctx.exec_id # The unique execution id. 

        exec_out_dir = f"{self.tmp_dir}/{exec_id}"

        # 0. Validate arguments 
        validated_args = SubProcessIngestionTaskArgs.parse_obj(args)

        # 1. Resolve the recipe (combine it with others)
        recipe: dict = SubProcessTaskUtil._resolve_recipe(validated_args.recipe, ctx, self.ctx)

        # 2. Write recipe file to local FS (requires write permissions to /tmp directory)
        file_name: str = "recipe.yml"  
        SubProcessTaskUtil._write_recipe_to_file(exec_out_dir, file_name, recipe)

        # 3. Spin off subprocess to run the run_ingest.sh script
        datahub_version = validated_args.version # The version of DataHub CLI to use. 
        plugins = recipe["source"]["type"] # The source type -- ASSUMPTION ALERT: This should always correspond to the plugin name. 
        debug_mode = validated_args.debug_mode
        command_script: str = "run_ingest.sh" # TODO: Make sure this is EXECUTABLE before running

        stdout_lines: deque[str] = deque(maxlen=self.config.max_log_lines)
        full_log_file = open(f"{self.config.log_dir}/ingestion-{exec_id}.txt", "w")

        report_out_file = f"{exec_out_dir}/ingestion_report.json"

        logger.info(f'Starting ingestion subprocess for exec_id={exec_id} ({plugins})')
        ingest_process = await asyncio.create_subprocess_exec(
            *[command_script, exec_id, datahub_version, plugins, self.tmp_dir, f"{exec_out_dir}/recipe.yml", report_out_file, debug_mode],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=SubProcessTaskUtil.SUBPROCESS_BUFFER_SIZE,
        )

        # 4. Monitor and report progress.
        most_recent_log_ts: Optional[datetime] = None

        async def _read_output_lines() -> None:
            create_new_line = True
            while True:
                assert ingest_process.stdout

                # We can't use the readline method directly.
                # When the readline method hits a LimitOverrunError, it will
                # discard the line or possibly the entire buffer.
                try:
                    line_bytes = await ingest_process.stdout.readuntil(b'\n')
                except asyncio.exceptions.IncompleteReadError as e:
                    # This happens when we reach the end of the stream.
                    line_bytes = e.partial
                except asyncio.exceptions.LimitOverrunError as e:
                    line_bytes = await ingest_process.stdout.read(SubProcessTaskUtil.MAX_BYTES_PER_LINE)
                
                # At this point, if line_bytes is empty, then we're at EOF.
                # If it ends with a newline, then we successfully read a line.
                # If it does not end with a newline, then we hit a LimitOverrunError
                # and it contains a partial line.

                if not line_bytes:
                    logger.info(f'Got EOF from subprocess exec_id={exec_id} - stopping log monitor')
                    break
                line = line_bytes.decode("utf-8")
                
                nonlocal most_recent_log_ts
                most_recent_log_ts = datetime.now()

                full_log_file.write(line)

                if create_new_line:
                    stdout_lines.append("")
                    sys.stdout.write(f'[{exec_id} logs] ')
                create_new_line = line.endswith('\n')

                current_line_length = len(stdout_lines[-1])
                if current_line_length == 0:
                    # If we're at the beginning of a new line, scan for the error signature
                    # of memory issues.
                    if line.startswith('/usr/local/bin/run_ingest.sh:') and 'Killed' in line:
                        ctx.get_report().report_error('The ingestion process was killed, likely because it ran out of memory. You can resolve this issue by allocating more memory to the datahub-actions container.')
                if current_line_length < SubProcessTaskUtil.MAX_BYTES_PER_LINE:
                    allowed_length = SubProcessTaskUtil.MAX_BYTES_PER_LINE - current_line_length
                    if len(line) > allowed_length:
                        trunc_line = f"{line[:allowed_length]} [...truncated]\n"
                    else:
                        trunc_line = line

                    stdout_lines[-1] += trunc_line
                    sys.stdout.write(trunc_line)
                    sys.stdout.flush()
                else:
                    # If we've already reached the max line length, then we simply ignore the rest of the line.
                    pass

                await asyncio.sleep(0)
        
        async def _report_progress() -> None:
            while True:
                if ingest_process.returncode is not None:
                    logger.info(f'Detected subprocess return code exec_id={exec_id} - stopping logs reporting')
                    break

                await asyncio.sleep(self.config.heartbeat_time_seconds)

                # Report progress
                if ctx.request.progress_callback:
                    if most_recent_log_ts is None:
                        report = 'No logs yet'
                    else:
                        report = SubProcessTaskUtil._format_log_lines(stdout_lines)
                        current_time = datetime.now()
                        if most_recent_log_ts < current_time - timedelta(minutes=2):
                            message = f'WARNING: These logs appear to be stale. No new logs have been received since {most_recent_log_ts} ({(current_time - most_recent_log_ts).seconds} seconds ago). ' \
                                "However, the ingestion process still appears to be running and may complete normally."
                            report = f'{report}\n\n{message}'

                    # TODO maybe use the normal report field here?
                    logger.debug(f'Reporting in-progress for exec_id={exec_id}')
                    ctx.request.progress_callback(report)

                full_log_file.flush()
                await asyncio.sleep(0)
        
        async def _process_waiter() -> None:
            await ingest_process.wait()
            logger.info(f'Detected subprocess exited exec_id={exec_id}')
        
        read_output_task = asyncio.create_task(_read_output_lines())
        report_progress_task = asyncio.create_task(_report_progress())
        process_waiter_task = asyncio.create_task(_process_waiter())

        try: 
            await tasks.gather(read_output_task, report_progress_task, process_waiter_task)
        except Exception as e:
            # This could just be a normal cancellation or it could be that
            # one of the monitoring tasks threw an exception.
            # In this case, we should kill the subprocess and cancel the other tasks.

            ingest_process.terminate()

            # If the cause of the exception was a cancellation, then this is a no-op
            # because the gather method already propagates the cancellation.
            read_output_task.cancel()
            report_progress_task.cancel()
            process_waiter_task.cancel()

            # Using return_exceptions=True here so that we wait for everything
            # to get cancelled completely before re-raising the exception.
            # If we used return_exceptions=False, this would throw immediately.
            await tasks.gather(ingest_process.wait(), read_output_task, report_progress_task, process_waiter_task, return_exceptions=True)

            if isinstance(e, asyncio.CancelledError):
                # If it was a cancellation, then we re-raise.
                raise
            else:
                raise RuntimeError(f'Something went wrong in the subprocess executor: {e}') from e
        finally:
            full_log_file.close()

            if os.path.exists(report_out_file):
                with open(report_out_file,'r') as structured_report_fp:
                    ctx.get_report().set_structured_report(structured_report_fp.read())

            ctx.get_report().set_logs(SubProcessTaskUtil._format_log_lines(stdout_lines))
            
            # Cleanup by removing the recipe file
            SubProcessTaskUtil._remove_directory(exec_out_dir)
        
        return_code = ingest_process.returncode
        if return_code != 0:
            # Failed
            ctx.get_report().report_info("Failed to execute 'datahub ingest'")
            raise TaskError("Failed to execute 'datahub ingest'") 
        
        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub ingest'")


    def close(self) -> None:
        pass

    

