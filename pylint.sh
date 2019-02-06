#!/usr/bin/env bash

if ! hash pylint >/dev/null 2>&1
then
    echo "pylint shall be installed"
    exit 1
fi

sources_path=${1:-ikats}
pylint --rcfile pylint.rc ikats --ignore="tests"