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
from typing import List
import os
import shutil
import yaml
import json
from typing import Sequence
import re
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.execution.task import TaskError

logger = logging.getLogger(__name__)


class SubProcessTaskUtil:

    MAX_LOG_LINES = 2000

    # The original value is 64kb (https://github.com/python/cpython/blob/7528e2c06c8baf809b56f406bcc50e8436c9647c/Lib/asyncio/streams.py#L23).
    # Increasing it should improve performance.
    SUBPROCESS_BUFFER_SIZE = 2 ** 20  # 1mb

    # GMS / mysql has a 4mb limit on the size of a data packet.
    # Doing 90% of that so we have some buffer.
    MAX_LOG_SIZE_BYTES = int(0.9 * 2 ** 22)  # 90% of 4mb

    # We want to truncate long lines so that we can show more lines in the logs.
    MAX_BYTES_PER_LINE = 2 ** 12  # 4kb

    @staticmethod
    def _format_log_lines(lines: Sequence[str]) -> str: 
        text = "".join(lines)

        # Python slices are super permissive on index bounds, so this works.
        text = text[-SubProcessTaskUtil.MAX_LOG_SIZE_BYTES:]

        if len(lines) >= SubProcessTaskUtil.MAX_LOG_LINES:
            # lines is a deque, so len(lines) won't be larger than MAX_LOG_LINES.
            text = f"[earlier logs truncated...]\n{text}"

        return text

    @staticmethod
    def _resolve_secrets(secret_names: List[str], ctx: ExecutorContext) -> dict:
        # Attempt to resolve secret using by checking each configured secret store.
        secret_stores = ctx.get_secret_stores()
        final_secret_values = dict({})

        for secret_store in secret_stores:
            try: 
                # Retrieve secret values from the store. 
                secret_values_dict = secret_store.get_secret_values(secret_names)
                # Overlay secret values from each store, if not None. 
                for secret_name, secret_value in secret_values_dict.items():
                    if secret_value is not None:
                        final_secret_values[secret_name] = secret_value
            except Exception:
                logger.exception(f"Failed to fetch secret values from secret store with id {secret_store.get_id()}")
        return final_secret_values

    @staticmethod
    def _resolve_recipe(recipe: str, execution_ctx: ExecutionContext, executor_ctx: ExecutorContext) -> dict:
        # Now attempt to find and replace all secrets inside the recipe. 
        secret_pattern = re.compile('.*?\${(\w+)}.*?')

        resolved_recipe = recipe
        secret_matches = secret_pattern.findall(resolved_recipe)

        # 1. Extract all secrets needing resolved. 
        secrets_to_resolve = []
        if secret_matches:
            for match in secret_matches:
                secrets_to_resolve.append(match)

        # 2. Resolve secret values
        secret_values_dict = SubProcessTaskUtil._resolve_secrets(secrets_to_resolve, executor_ctx)

        # 3. Substitute secrets into recipe file
        if secret_matches:
            for match in secret_matches:
                # a. Check if secret was successfully resolved.
                secret_value = secret_values_dict.get(match)
                if secret_value is None:
                    # Failed to resolve secret. 
                    raise TaskError(f"Failed to resolve secret with name {match}. Aborting recipe execution.")
                    
                # b. Substitute secret value. 
                resolved_recipe = resolved_recipe.replace(
                    f'${{{match}}}', secret_value
                )

        json_recipe = json.loads(resolved_recipe)

        # Inject run_id into the recipe
        json_recipe["run_id"] = execution_ctx.exec_id

        # TODO: Inject the ingestion source id as well.

        # For now expect that the recipe is complete, this may not be the case, however for hybrid deployments. Secret store!
        return json_recipe

    @staticmethod
    def _write_recipe_to_file(dir_path: str, file_name: str, recipe: dict) -> None:

        # 1. Create directories to the path
        os.makedirs(dir_path, mode = 0o777, exist_ok = True)

        # 2. Dump recipe dictionary to a YAML string
        yaml_recipe = yaml.dump(recipe)

        # 3. Write YAML to file  
        file_handle = open(dir_path + "/" + file_name, "w")
        n = file_handle.write(yaml_recipe)
        file_handle.close()

    @staticmethod
    def _remove_directory(dir_path: str) -> None:
        shutil.rmtree(dir_path)