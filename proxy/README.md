# Proxy

- [Deployment Instructions](#deployment-instructions)
  - [TLS Key and Certificate Generation](#tls-key-and-certificate-generation)
  - [Configuration File Preparation](#configuration-file-preparation)
  - [Proxy Startup](#proxy-startup)

## Deployment Instructions


To deploy a Proxy, you will need:

* A TLS key and a TLS certificate that is generated using the BK Node Owner’s keys
* A file containing the Block Keeper Node Owner keys (required to generate the TLS certificate)
* A configuration file

### TLS Key and Certificate Generation
The Proxy operates over the QUIC+TLS protocol, so it must have its own TLS key and TLS certificate that is generated using the BK Node Owner’s keys.

**Important:**  
The certificate will not be accepted until the BK that issued it is included in the [BK set](https://docs.ackinacki.com/glossary#bk-set).
Once that happens, there is no need to reissue the certificate.

To generate them, use the `gen_certs` command:  

```txt
Usage: gen_certs [OPTIONS] --name <NAME> 

Options:
  -s, --subjects <SUBJECTS>            Comma-separated list of certificate subjects (e.g. 'localhost,*.example.com,127.0.0.1')
  -n, --name <NAME>                    Certificate name (e.g. client)
  -o, --output-dir <OUTPUT_DIR>        Output directory (default: current directory)
      --ed-key-secret <ED_KEY_SECRET>  OOptional secret key of the Block Keeper’s owner wallet key pair (64-char hex). If specified, omit `ed_key_path`
      --ed-key-path <ED_KEY_PATH>      Optional path to the Block Keeper’s owner wallet key file, stored as JSON `{ "public": "64-char hex", "secret": "64-char hex" }`. If specified, omit `ed_key_secret`
  -f, --force                          Overwrite existing files
  -h, --help                           Print help
  -V, --version                        Print version
```

For example:  
```bash
gen_certs -n client -o ./certs -s "localhost,*.example.com,127.0.0.1" --ed-key-path ./bk_wallet/bk_wallet.keys.json
```

After running the command, the `./certs` directory will contain:  
* `proxy.ca.pem` — the TLS certificate
* `proxy.key.pem` — the private key

These files must then be specified in the Proxy configuration (`my_cert` and `my_key`).

Certificates will be self-signed and will include the following subjects: `localhost`, `*.example.com`, and `127.0.0.1` (any other subjects will be rejected).

### Configuration File Preparation

```yaml
# proxy_config.yaml
---
# Socket address listen on.
bind: 0.0.0.0:10000

# Public socket address, used to connect to this Proxy
# Must match the address specified in the Block Keeper host configuration  
my_addr: <SocketAddr>

# Path to the TLS certificate PEM file, generated with `gen_certs`
# Certificate should be generated with owner's BK wallet keys
my_cert: proxy.ca.pem

# Path to the TLS private key PEM file, generated with `gen_certs`
my_key: proxy.key.pem

# Optional trusted TLS certificates
# Usually Proxy uses trusted pubkeys from gossip cluster and this field is empty
peer_certs:
  - certs/peer1.ca.pem

# Optional trusted BK wallet pubkeys
# Usually Proxy gets trusted pubkeys from gossip cluster and this field is empty
peer_ed_pubkeys:
  - hex-64-pubkey

# Socket addresses of owner's node
# Used to get an actual BK set
bk_addrs:
  - 1.2.3.4:8600

# Predefined list for subscribing.
# If specified, then Proxy will not use gossip and BK set.
# Usually this list is empty, so Proxy builds a subscribe list from the actual BK set and gossip cluster
subscribe:
  - 1.2.3.4:8600

# Gossip configuration
gossip:
  # UDP socket address to listen gossip.
  # Defaults to "127.0.0.1:10000"
  listen_addr: 0.0.0.0:10000

  # Public gossip advertises socket address.
  # Defaults to `listen_addr` address
  advertise_addr: 1.2.3.4:10000

  # Gossip seed nodes socket addresses.
  seeds:
    - 1.2.3.4:10001

  # Chitchat cluster id for gossip
  # Defaults to: acki_nacki
  cluster_id: acki_nacki
```

### Proxy Startup

```txt
Usage: proxy [OPTIONS]

Options:
  -c, --config-path <CONFIG_PATH>  [default: config.yaml]
  -h, --help                       Print help
  -V, --version                    Print version
```

Example command:  
```bash
proxy -c path/to/proxy_config.yaml
```
