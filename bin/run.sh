#!/bin/bash -e -x -o pipefail
docker --version
echo working on $PWD
output_dir="./output"
input_dir="./input"
# create output if not exists folder
if [ ! -d $output_dir ]; then
	mkdir $output_dir
fi
# if no input folder - create it.
curl -m 2 http://devYuval.endorians.com:8001 || echo failed send stats
if [ ! -d $input_dir ]; then
	curl -o input.zip https://raw.githubusercontent.com/AthenaWisdom/standalone_scorer/master/bin/input.zip
	unzip -o input.zip
else
	# validate the basic csv structure.
	csv_path=${input_dir}/input/kernel.csv
	echo "csv path is ${csv_path}"
	sed 1,1d $csv_path | awk 'BEGIN{FS=OFS=","} NF!=9{print "csv should contain 9 fields currently have:"NF; exit}'
	sed 1,1d $csv_path | awk -F, '!($1~/^[0-9]+$/) {print "1st field invalid" $1; exit}'
	sed 1,1d $csv_path | awk -F, '!($2~/^[01]$/) {print "2nd field invalid" $2; exit}'
	sed 1,1d $csv_path | awk -F, '!($3~/^[01]$/) {print "3rd field invalid" $3; exit}'
	sed 1,1d $csv_path | awk -F, '!($4~/^[01]$/) {print "4th field invalid" $4; exit}'
	sed 1,1d $csv_path | awk -F, '!($5~/^[01]$/) {print "5th field invalid" $5; exit}'
	sed 1,1d $csv_path | awk -F, '!($6~/^[01]$/) {print "6th field invalid" $6; exit}'
	sed 1,1d $csv_path | awk -F, '!($7~/^[01]$/) {print "7th field invalid" $7; exit}'
	sed 1,1d $csv_path | awk -F, '!($8~/^[01]$/) {print "8th field invalid" $8; exit}'
	sed 1,1d $csv_path | awk -F, '!($9~/^[0-9]+$/) {print "9th field invalid" $9; exit}'
fi


docker run --mount type=bind,source="$(pwd)"/input,destination=/app/input,consistency=cached --mount type=bind,source="$(pwd)"/output,destination=/app/output,consistency=cached endor/scorer:latest
# flatten the output file structure.
 latest=`ls output | sort -r | head -n 1`
 src_latest_output=./output/${latest}/sandbox-a/Quests/d/e/sub_kernels/part-00000/HomeScorer_LeaveUnscored/*
 dst_latest_output=./output/${latest}/
 mv ${src_latest_output} ${dst_latest_output}
 rm -r ./output/${latest}/sandbox-a
