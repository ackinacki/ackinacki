# Default target
.DEFAULT_GOAL := build

# Compiler and options
SOLD ?= $(shell command -v sold || echo ../compiler/sold)
TVM_CLI ?= $(shell command -v tvm-cli || echo ../compiler/tvm-cli)
TVM_DEBUGGER ?= $(shell command -v tvm-debugger || echo ../compiler/tvm-debugger)
TVM_VERSION ?= gosh

# Default variables for customization
TVCS ?= # Define in folder-specific Makefile
CPFL ?= # Define in folder-specific Makefile

# Derived variables
SRC_FILES := $(addsuffix .sol, $(TVCS))
TVC_FILES := $(addsuffix .tvc, $(TVCS))
COPIED_FILES := $(addsuffix .sol, $(CPFL))

# Clean target: removes all generated files
clean:
	@echo "Cleaning up generated files..."
	rm -f ./*.tvc ./*.abi.json ./*.code ./*.debug.json
	rm -rf ./.solc_imports ./.sold_imports
	@echo "Clean complete."

# Target to build all contracts
build: copy-files build-contracts

# Copy dependencies from the parent directory
copy-files: $(COPIED_FILES)

# Rule to copy files from the parent directory
%.sol:
	@echo "Copying $@ from parent directory..."
	cp ../$@ . || { echo "Failed to copy $@"; exit 1; }

# Build contracts: compile all specified TVCS contracts
build-contracts: $(TVC_FILES)

# Pattern rule to compile .sol to .tvc and .abi.json
./%.tvc ./%.abi.json: ./%.sol
	@echo "Compiling $<..."
	$(SOLD) --tvm-version $(TVM_VERSION) $< -o . || { echo "Compilation failed for $<"; exit 1; }
	rm -f ./$*.debug.json ./$*.code
	@echo "Compiled $< successfully."
