# Release Notes

All notable changes to this project will be documented in this file.

## [0.3.4] – 2024-01-13

## New
- Getter for Signer Index address

## [0.3.3] – 2024-01-08

## New
- `bm/v2/messages` api with synchronous message processing

## Improvements
- Refactor block state markers and state storage organization
  
## Fixed
- Attestation sending on rotate

## [0.3.2] – 2024-12-30

## New
- Proxy service 

## Fixed
- Signer index integration
- Fixed some Shellnet bugs

## [0.3.1] – 2024-12-30

## New
- Slashing
- Add log rotate to public deployments BK and BM 
  
## Improved
- Change DNS name to IPs in storage 
- Adjust real network node parameters
- Tracing spans added

## Fixed
- Node join 
- Share state
  
## [0.3.0] – 2024-12-12

## New
- Block Manager deployment documentation and scripts
- Thread split supported
- `tvm-tracing` feature added to trace tvm execution results
- `allow-dappid-thread-split` feature added to enable possibility to split into threads inside Dapp ID
- Getter for `ProxyListCode` added
- Added node binaries of 2 new types into node image: 
  - with `tvm-tracing` feature enabled
  - with `allow-dappid-thread-split` and `tvm_tracing` features enabled 
  
 ## Improved

- BK API  root URL is now `bk/v1` with 1 endpoint `bk/v1/messages` 
  that can receive POST requests with external messages (previously `topic/requests`) 
- Message Router API root URL is now `bm/v1` with 1 endpoint `bm/v1/messages` 
  that can receive POST requests with external messages. 
  This renaming is a preparation step before moving this component into Block Manager in the next releases and deprecation of Message Router.


## [0.2.0] – 2024-11-28

## New

- Static Multithreading supported
- GraphQL API is supported in Block Manager (only block indexer, Accounts API is not implemented yet)
- Block Manager ansible scripts and documentation

## [0.1.2] – 2024-11-11

### Improved

Node protocol improvements

## [0.1.1] – 2024-10-15

### Improved

Node protocol improvements

## [0.1.0] – 2024-10-04

### New

Initial release
