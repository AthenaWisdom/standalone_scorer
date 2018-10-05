# standalone_scorer


prerequisite:
Docker CE : https://docs.docker.com/install/

in order to use:
 - bash <(curl -s https://raw.githubusercontent.com/AthenaWisdom/standalone_scorer/master/bin/run.sh)


Important files:

./in/in/universe.txt -> The entire population one wants to label. The file is a CSV format and should maintain this naming convention.
./in/in/whites_example.txt -> A “look alikes” subpopulation: given as a sample to the engine, in order to find more like these (i.e, the grounds). The file is a CSV format and should maintain this naming convention.

Important notes:
sphere (all data) has to include universe entities, which has to include whites entities.


