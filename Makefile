.PHONY: check fmt vet test interop redis bench

# Everything a change must pass before it is merged.
check: fmt vet test

fmt:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

vet:
	go vet ./...
	go vet -tags interop ./test/interop

test:
	go test -race -count=1 ./...

# Runs the official JavaScript client against the server. Needs Node.js.
interop:
	cd test/interop && npm ci --no-audit --no-fund
	go test -tags interop -count=1 -v ./test/interop

# Runs the Redis adapter tests against a Redis server, 127.0.0.1:6379 unless
# REDIS_ADDR says otherwise. Without a reachable server the tests that need
# one are skipped and the offline tests still run.
redis:
	go test -race -count=1 -v ./redisadapter

bench:
	go test -run '^$$' -bench . -benchmem ./parser
