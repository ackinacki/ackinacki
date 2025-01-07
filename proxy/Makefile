.PHONY: build kill up

build:
	docker build -t acki-nacki-proxy-local .

kill:
	docker compose kill

up:
	docker compose up -d

pmanager_test:
	sed -i '' -e 's/enabled: true/enabled: false/g' config.yaml
	RUST_LOG=debug cargo run --bin proxy_manager -- -c config.yaml docker --container 123
