#!/bin/bash -x
rm -rf ./input
cp -R ../in ./
mv ./in ./input
mv ./input/in ./input/input
docker run --mount type=bind,source="$(pwd)"/input,destination=/app/input,consistency=cached --mount type=bind,source="$(pwd)"/output,destination=/app/output,consistency=cached endor/scorer:latest

