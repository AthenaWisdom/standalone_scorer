#!/bin/bash -x
rm -rf ./input
mkdir output
curl -o input.zip https://raw.githubusercontent.com/AthenaWisdom/standalone_scorer/master/bin/input.zip
unzip -o input.zip
docker run --mount type=bind,source="$(pwd)"/input,destination=/app/input,consistency=cached --mount type=bind,source="$(pwd)"/output,destination=/app/output,consistency=cached endor/scorer:latest

