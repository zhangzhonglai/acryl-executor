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

import datetime
import json
import logging
import requests
import requests.adapters

from datahub.ingestion.graph.client import DataHubGraph 
from json.decoder import JSONDecodeError
from requests.exceptions import HTTPError, RequestException
from typing import Dict, List, Optional, Tuple, Union

# Class used to fetch Secrets from DataHub. 
class DataHubSecretsClient:

    _graph: DataHubGraph

    def __init__(
        self,
        graph
    ):
        self._graph = graph

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        if len(secret_names) == 0:
            return {}

        request_json = {
            "query": """query getSecretValues($input: GetSecretValuesInput!) {\n
                getSecretValues(input: $input) {\n
                    name\n
                    value\n
                }\n
            }""",
            "variables": {
                "input": {
                    "secrets": secret_names
                }
            }
        }

        # Fetch secrets using GraphQL API f
        response = self._graph._session.post(
            f"{self._graph.config.server}/api/graphql", json=request_json
        )
        response.raise_for_status()

        # Verify response 
        res_data = response.json()
        if "errors" in res_data:
            raise Exception("Failed to retrieve secrets from DataHub.")

        # Convert list of name, value secret pairs into a dict and return 
        secret_value_list = res_data["data"]["getSecretValues"]
        secret_value_dict = dict()
        for secret_value in secret_value_list:
            secret_value_dict[secret_value["name"]] = secret_value["value"]
        return secret_value_dict