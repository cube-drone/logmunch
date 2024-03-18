default:
	cat Makefile

start:
	ROCKET_PORT="9283" \
	ROCKET_ADDRESS="0.0.0.0" \
	ROCKET_SECRET_KEY="grPMXJszWoqdlGv1M7Y+9f0FsPhxsycGVjuQ6mfobIk=" \
	cargo run

test:
	cargo test -- --nocapture

hardtest:
	cargo test --release -- --nocapture