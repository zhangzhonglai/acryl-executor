#!/bin/bash
# usage: ./run_test_connection.sh <task-id> <datahub-version> <plugins-required> <tmp-dir> <recipe-file> <report-file>

set -euo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit

source ingestion_common.sh

task_id=$1
datahub_version="$2"
plugins_required="$3"
tmp_dir="$4"
recipe_file="$5"
report_file="$6"

create_venv $task_id $datahub_version $plugins_required $tmp_dir
echo "recipe is at $recipe_file"
if (datahub ingest run --help | grep test-source-connection); then
  echo "Success"
  rm -f "$report_file"
  # Execute DataHub recipe, based on the recipe id. 
  if (datahub ingest -c "${recipe_file}" --test-source-connection --report-to "${report_file}"); then
    exit 0
   else
    exit 1
fi
else
  echo "datahub ingest doesn't seem to have test_connection feature. You are likely running an old version"
  cat << EOF > "$report_file"
{
  "internal_failure": true,
  "internal_failure_reason": "datahub library doesn't have test_connection feature. You are likely running an old version."
}
EOF
  exit 0 # success here means we have succeeded at trying and we know why we failed
fi
