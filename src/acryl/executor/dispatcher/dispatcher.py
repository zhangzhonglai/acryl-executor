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

from acryl.executor.execution.executor import Executor
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.request.signal_request import SignalRequest 
from abc import abstractmethod

# An abstract base class representing a dispatcher.
class Dispatcher:

    @abstractmethod
    def can_dispatch(self, request: ExecutionRequest) -> bool: 
        """
        Returns true if the execution request can be dispatched, false otherwise. 
        """

    @abstractmethod
    def dispatch(self, request: ExecutionRequest) -> None: 
        """
        Dispatch the task execution request 
        """

    @abstractmethod
    def dispatch_signal(self, request: SignalRequest) -> None: 
        """
        Dispatch the signal request
        """