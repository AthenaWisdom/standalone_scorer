# Endor Standalone Scorer


## Prerequisites
Docker CE : https://docs.docker.com/install/

## Usage
 - bash <(curl -s https://raw.githubusercontent.com/AthenaWisdom/standalone_scorer/master/bin/run.sh)


# Terminology

**Kernel** - a table containing Universe, Whites and Ground,

**Universe** - The entire population one wants to label.

**Whites** - A “look alikes” subpopulation: given as a sample to the engine, in order to find more like these (i.e, the grounds).

**Grounds** - The ‘Ground Truth’: the subpopulation one is actually seeking. 


## Important files

./in/in/kernel.csv -> The entire population one wants to label. The file is a CSV format and should maintain this naming convention.


The file is a CSV format and should maintain this naming convention.

## Important notes
sphere (all data) has to include universe entities, which has to include whites entities.


