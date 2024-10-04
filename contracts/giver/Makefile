.DEFAULT_GOAL := build

tvcs = GiverV3

clean:
	rm -f *.tvc *.abi.json *.code *.debug.json
	rm -rf .solc_imports
	rm -rf .sold_imports

build-contracts:
	for i in $(tvcs); do \
		sold --tvm-version gosh $$i.sol || exit $$@; \
	done
	rm *.debug.json;
	#rm *.code;

build: clean build-contracts
