# syntax=docker/dockerfile:1.12

FROM docker.gosh.sh/rust AS builder
WORKDIR /build
COPY --link . ./
RUN \
  --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/target,id=target \
  <<EOF
    set -e

    echo "build all binaries"
    mold --run cargo build --release --target-dir=/target

    echo binaries: `cargo build --bin 2>&1 | sed -e '1,2d' | xargs`

    # clean
    rm -rf /usr/local/bin/*

    for binary in `cargo build --bin 2>&1 | sed -e '1,2d' | xargs`; do
        cp /target/release/"$binary" /usr/local/bin/
    done

    # debug
    ls -la /usr/local/bin
EOF

FROM docker.gosh.sh/debian AS final-base
WORKDIR /workdir
COPY --from=builder /usr/local/bin/* /usr/local/bin/
RUN gen_certs -o certs -n server -s "proxy,ackinacki.org,*.ackinacki.org"
RUN gen_certs -o certs -n client
