#!/usr/bin/env bash
set -euo pipefail

# Make sure docker is installed before executing it.

docker pull mattjtodd/raml2html:latest
docker run --rm -v $PWD:/raml mattjtodd/raml2html -i /raml/server/api/api.raml -o /raml/docs/api.html

