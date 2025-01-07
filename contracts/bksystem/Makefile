.DEFAULT_GOAL := build

tvcs = SignerIndex BLSKeyIndex BlockKeeperContractRoot AckiNackiBlockKeeperNodeWallet AckiNackiBlockKeeperNodeWalletConfig BlockKeeperEpochContract BlockKeeperCoolerContract BlockKeeperPreEpochContract BlockKeeperEpochProxyList 
clean:
	rm -f *.tvc *.abi.json *.code *.debug.json
	rm -rf .solc_imports
	rm -rf .sold_imports

build-contracts:
	for i in $(cpfl); do \
		cp ../$$i.sol . || exit $$@; \
	done
	for i in $(tvcs); do \
		$$(command -v sold || echo ../compiler/sold) --tvm-version gosh $$i.sol || exit $$@; \
	done
	rm -r *.debug.json;
	rm -r *.code;

build: clean build-contracts
