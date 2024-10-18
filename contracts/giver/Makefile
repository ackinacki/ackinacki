.DEFAULT_GOAL := build

tvcs = GiverV3
KEY_PATH='../../tests/GiverV3.keys.json'
GIVER_ADDRESS='0:1111111111111111111111111111111111111111111111111111111111111111'

clean:
	rm -f *.tvc *.abi.json *.code *.debug.json
	rm -rf .solc_imports
	rm -rf .sold_imports

build-contracts:
	for i in $(tvcs); do \
		$$(command -v sold || echo ../compiler/sold) --tvm-version gosh $$i.sol || exit $$@; \
	done
	rm *.debug.json;
	#rm *.code;

add_key:
	../compiler/tvm-cli -j genaddr --abi GiverV3.abi.json --setkey $(KEY_PATH) --save GiverV3.tvc

call_constructor_offchain:
	../compiler/tvm-debugger -a GiverV3.abi.json -m constructor -i GiverV3.tvc --sign $(KEY_PATH) --address $(GIVER_ADDRESS)

build: clean build-contracts add_key call_constructor_offchain
