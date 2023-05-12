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

from typing import Callable, List, Union, Optional
from pydantic import BaseModel

ExecutionProgressCallback = Callable[[str], None]


class ExecutionRequest(BaseModel):
    # Optional executor id to target. Fallbacks back the default (local) executor.
    executor_id: str = "default"

    # Optional run execution id provided by the caller
    exec_id: Union[str, None]

    # The name of the task to be executed
    name: str

    # Arguments to provide the task to be executed
    args: dict

    # Callback to report progress.
    progress_callback: Optional[ExecutionProgressCallback] = None
