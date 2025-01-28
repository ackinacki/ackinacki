
KEY_PATH='../../tests/GiverV3.keys.json'
GIVER_ADDRESS='0:1111111111111111111111111111111111111111111111111111111111111111'
TVCS := GiverV3

include ../Makefile.inc

add_key:
	../compiler/tvm-cli -j genaddr --abi GiverV3.abi.json --setkey $(KEY_PATH) --save GiverV3.tvc

call_constructor_offchain:
	../compiler/tvm-debugger -a GiverV3.abi.json -m constructor -i GiverV3.tvc --sign $(KEY_PATH) --address $(GIVER_ADDRESS)

.DEFAULT_GOAL := build-giver

build-giver: build add_key call_constructor_offchain

