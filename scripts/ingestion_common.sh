#!/bin/bash

function create_venv {
    task_id=$1
    datahub_version="$2"
    plugin="$3"
    tmp_dir="$4"
    # we always install datahub-rest and datahub-kafka in addition to the plugin
    all_plugins="datahub-rest,datahub-kafka,${plugin}"

    extra_deps=()
    if [ "$plugin" = "hive" ]; then
        # Add dependency 
        extra_deps+=("databricks-dbapi")
    fi

    venv_dir="$tmp_dir/venv-$plugin-${datahub_version}"

    echo 'Obtaining venv creation lock...'
    (
        flock --exclusive 200
        echo 'Acquired venv creation lock'
        SECONDS=0

        # Create tmp file to store requirements using provided recipe id. 
        req_file="$tmp_dir/requirements-${task_id}.txt"
        touch "$req_file"
        if [ "$datahub_version" == "latest" ]; then
            echo "Latest version will be installed"
            echo "acryl-datahub[${all_plugins}]" > "$req_file"
        else
            echo "acryl-datahub[${all_plugins}]==$datahub_version" > "$req_file"
        fi
        # Install extra deps
        if [ ${#extra_deps[@]} -ne 0 ]; then
            for extra_dep in ${extra_deps[@]}; do
                echo "$extra_dep" >> "$req_file"
            done
        fi
        if [ ! -d "$venv_dir" ]; then
            echo "venv doesn't exist.. minting.."
            python3 -m venv $venv_dir
            source "$venv_dir/bin/activate"
            pip install --upgrade pip wheel setuptools
            pip install -r $req_file
        else
            source "$venv_dir/bin/activate"
            if [ "$datahub_version" == "latest" ]; then
                # in case we are installing latest, we want to always re-install in case latest has changed
                pip install --upgrade -r $req_file
            fi
        fi
        rm $req_file

        echo "venv setup time = $SECONDS"
    ) 200>"$venv_dir.lock"

    source "$venv_dir/bin/activate"
}