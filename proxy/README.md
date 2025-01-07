# Proxy

## TLS Key/Certificate generation

```txt
Usage: gen_certs [OPTIONS] --name <NAME>

Options:
  -s, --subjects <SUBJECTS>      Comma-separated list of certificate subjects (e.g. 'localhost,*.example.com,127.0.0.1')
  -n, --name <NAME>              Certificate name (e.g. client)
  -o, --output-dir <OUTPUT_DIR>  Output directory (default: current directory)
  -f, --force                    Overwrite existing files
  -h, --help                     Print help
  -V, --version                  Print version
```

### Example

```bash
gen_certs --name client -o ./certs --subjects "localhost,*.example.com,127.0.0.1"
```

Will generate two files: `client.ca.pem` and `client.key.pem` in the `./certs` directory.
Certificates will be self-signed and will have the following subjects: `localhost`, `*.example.com` and `127.0.0.1` (other subjects will be rejected).

## Proxy config

```txt
Usage: proxy [OPTIONS]

Options:
  -b, --bind <BIND>                [default: 0.0.0.0:8080]
  -c, --config-path <CONFIG_PATH>  [default: config.yaml]
  -h, --help                       Print help
  -V, --version                    Print version
```

NOTE: `--bind` suppose to be socket address not an url

Example command:

```bash
proxy -b 0.0.0.0:8080 -c path/to/proxy_config.yaml
```

```yaml
# proxy_config.yaml
---
server:
  # path to PEM only TLS certificate of the proxy
  cert: certs/server.ca.pem
  # path to PEM only TLS key of the proxy
  key: certs/server.key.pem
connections:
  connection1:
    # if true, connection will receive data from the proxy
    subscriber: true
    # if true, connection is allowed to send data to the proxy
    publisher: true
    # tags do nothing for now
    tags:
      - tag1
    # path to PEM only TLS certificate of the destination client or proxy
    cert: connections/connection_name.ca.pem
    # outer connection type means that rather than waiting for connection from the client proxy will connect to the destination itself
    outer:
      # if `enabled: true` proxy periodically will attempt to connect to the destination itself
      # if `disable: false` proxy won't connect proactively but if connection already exists it will hold till enabled back and reuse TLS handshake when possible before re-handshake
      enabled: true
      url: https://localhost:4433/1/2/3
```

## Proxy Manager config

```txt
Acki Nacki Proxy Manager CLI

Usage: proxy_manager [OPTIONS] <COMMAND>

Commands:
  docker    Docker
  pid-path
  pid
  help      Print this message or the help of the given subcommand(s)

Options:
  -c, --proxy-config <PROXY_CONFIG>  Path to the proxy configuration file (should already exist) [default: ./proxy.yaml]
  -h, --help                         Print help
  -V, --version                      Print version
```

### Example of usage

```bash
proxy_manager -c path/to/config.yaml docker --container 'container_name'
# or
proxy_manager -c path/to/config.yaml pid --pid 1234
```

### Proxy list format (WIP)

Proxy manager receives a list of proxies to connect from the blockchain in JSON format:

```jsonc
[
  {
    "url": "https://www.example.com/path/path1", // full proxy url with protocol and path(optional)
    // content of the TLS certificate file in PEM format (e.g. like if it's for nginx or apache or openssl)
    "cert": "-----BEGIN CERTIFICATE-----
MIIBWzCCAQCgAwIBAgIUVQCMPQZ8PYCKYWzNeySt/DyTJAMwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWduZWQgY2VydDAgFw03NTAxMDEwMDAw
MDBaGA80MDk2MDEwMTAwMDAwMFowITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWdu
ZWQgY2VydDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABI4RYVCqhTLpm4uZ2g4O
B+w5mfDZvtqOwV7felR7ZFcxXu2lhEqVcd8I32RIqukSfJddopfrP7JQkwoVApDl
hkyjFDASMBAGA1UdEQQJMAeCBVs6OjFdMAoGCCqGSM49BAMCA0kAMEYCIQCEDCR4
fCECGL41JQnpQO+S89epyGbp96ij7/I/H1eR4wIhAI/VDdnc+8mUajOs3jswvp46
CtRDY1voE6J17J6sd3Os
-----END CERTIFICATE-----",
  },
  {
    "url": "https://www.example.com:4321/path/path2",
    "cert": "-----BEGIN CERTIFICATE-----
MIIBWzCCAQCgAwIBAgIUVQCMPQZ8PYCKYWzNeySt/DyTJAMwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWduZWQgY2VydDAgFw03NTAxMDEwMDAw
MDBaGA80MDk2MDEwMTAwMDAwMFowITEfMB0GA1UEAwwWcmNnZW4gc2VsZiBzaWdu
ZWQgY2VydDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABI4RYVCqhTLpm4uZ2g4O
B+w5mfDZvtqOwV7felR7ZFcxXu2lhEqVcd8I32RIqukSfJddopfrP7JQkwoVApDl
hkyjFDASMBAGA1UdEQQJMAeCBVs6OjFdMAoGCCqGSM49BAMCA0kAMEYCIQCEDCR4
fCECGL41JQnpQO+S89epyGbp96ij7/I/H1eR4wIhAI/VDdnc+8mUajOs3jswvp46
CtRDY1voE6J17J6sd3Os
-----END CERTIFICATE-----",
  }
]
```
