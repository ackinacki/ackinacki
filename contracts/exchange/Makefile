TVCS := Exchange

# Override compilation rule BEFORE include to add --base-path for cross-directory imports
./%.tvc ./%.abi.json: ./%.sol
	@echo "Compiling $<..."
	$(SOLD) --tvm-version $(TVM_VERSION) --base-path .. $< -o . || { echo "Compilation failed for $<"; exit 1; }
	rm -f ./$*.debug.json ./$*.code
	@echo "Compiled $< successfully."

include ../Makefile.inc
