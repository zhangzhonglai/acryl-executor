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

from acryl.executor.secret.secret_store import SecretStore
from acryl.executor.secret.datahub_secrets_client import DataHubSecretsClient
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from pydantic import BaseModel, validator
from typing import Union, Dict, List, Optional

logger = logging.getLogger(__name__)

class DataHubSecretStoreConfig(BaseModel):

    graph_client: Optional[DataHubGraph] = None
    graph_client_config: Optional[DatahubClientConfig] = None

    class Config:
        arbitrary_types_allowed = True

    @validator('graph_client')
    def check_graph_connection(cls, v):
        if v is not None:
            v.test_connection()
        return v


# An implementation of SecretStore that fetches secrets from DataHub
class DataHubSecretStore(SecretStore):

    # Client for fetching secrets from DataHub GraphQL API 
    client: DataHubSecretsClient

    def __init__(self, config):
        # Attempt to establish an outbound connection to DataHub and create a client.
        if config.graph_client is not None:
            self.client = DataHubSecretsClient(
                graph=config.graph_client
            )
        elif config.graph_client_config is not None: 
            graph = DataHubGraph(config.graph_client_config)
            self.client = DataHubSecretsClient(graph)
        else:
            raise Exception("Invalid configuration provided: unable to construct DataHub Graph Client.")

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        # Fetch the secret from DataHub, using the credentials provided in the configuration. 
        # Use the GraphQL API. 
        try:
            return self.client.get_secret_values(secret_names)
        except Exception:
            # Failed to resolve secrets, return empty. 
            logger.exception(f"Caught exception while attempting to fetch secrets from DataHub. Secret names: {secret_names}")
            return {}

    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        secret_value_dict = self.get_secret_values([secret_name])
        return secret_value_dict.get(secret_name)

    def get_id(self) -> str:
        return "datahub"

    @classmethod
    def create(cls, config):
        config = DataHubSecretStoreConfig.parse_obj(config)
        return cls(config)

