# Thread Reference State

This crate provides the `ThreadReferencesState` struct for managing cross-thread block references in the Acki Nacki node.

## Overview

The `ThreadReferencesState` maintains a map of thread identifiers to their most recent referenced blocks. It provides methods to:
- Query whether a set of blocks can be referenced (`can_reference`)
- Update the reference state when blocks are finalized (`move_refs`)
- Track implicit and explicit block references across threads

## Usage

The crate defines a trait `CrossThreadRefDataTrait` that must be implemented by the block reference data type used with `ThreadReferencesState`.
