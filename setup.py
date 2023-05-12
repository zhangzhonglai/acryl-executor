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

import os
from typing import Dict, Set
import setuptools

package_metadata: dict = {}
with open("./src/acryl/__init__.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()

    return description


base_requirements = {
    # Compatibility.
    "typing_extensions>=3.7.4; python_version < '3.8'",
    "mypy_extensions>=0.4.3",
    "sqlalchemy-stubs>=0.4",
    # Actual dependencies.
    "pydantic>=1.5.1",
    "acryl-datahub[datahub-rest]>=0.9.4",
}

aws_common = {
    # AWS Python SDK
    "boto3",
    # Deal with a version incompatibility between botocore (used by boto3) and urllib3.
    # See https://github.com/boto/botocore/pull/2563.
    "botocore!=1.23.0",
}

plugins: Dict[str, Set[str]] = {
    # Task plugins
    "aws-sqs-remote-executor": aws_common,
    # Secret store plugins 
    "aws-sm-secret-store": aws_common
}

mypy_stubs = {
    "types-dataclasses",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyYAML",
    "types-freezegun",
}

dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "flake8>=3.8.3",
    "flake8-tidy-imports>=4.3.0",
    "mypy>=0.901,<0.920",
    "pytest>=6.2.2",
    "requests-mock",
    "freezegun",
}

entry_points = {
    # Tasks 
    "datahub.executor.task.plugins": [
        "sub_process_ingestion_task = acryl.executor.execution.sub_process_ingestion_task:SubProcessIngestionTask"
    ]
    # todo: add secret store plugins also. 
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/",
        "Source": "https://github.com/linkedin/datahub",
        "Changelog": "https://github.com/linkedin/datahub/releases",
    },
    license="Apache License 2.0",
    description="An library used within Acryl Agents to execute tasks",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development"
    ],
    # Package info.
    scripts=['scripts/run_ingest.sh', 'scripts/run_test_connection.sh', 'scripts/ingestion_common.sh'],
    zip_safe=False,
    python_requires=">=3.7",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        "dev": list(dev_requirements)
    },
)
