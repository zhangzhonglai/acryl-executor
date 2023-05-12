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

from abc import abstractmethod
from typing import Dict, List, Union
from acryl.executor.common.config import ConfigModel

class SecretStoreConfig(ConfigModel):
    type: str
    config: Dict 

# Abstract base class for a Secret Store, or a class that resolves "secret" values 
# by name. 
class SecretStore:

    @classmethod
    @abstractmethod
    def create(cls, configs: dict) -> "SecretStore":
        pass

    @abstractmethod
    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        """
        Attempt to fetch a group of secrets, returning a Dictionary of the secret of None if one 
        cannot be resolved by the store. 
        """

    @abstractmethod
    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        """
        Attempt to fetch a secret, or return None if one cannot be resolved. 
        """

    @abstractmethod
    def get_id(self) -> str:
        """
        Get a unique name or id associated with the Secret Store. 
        """

    @abstractmethod
    def close(self) -> None:
        """
        Wraps up the task
        """