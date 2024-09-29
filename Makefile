.PHONY: profile.cpu
profile.cpu:
	go tool pprof -http=":8888" cpu.out

.PHONY: fix
fix:
	go fmt ./...

GOFMT_OUTPUT = $(shell gofmt -l .)

.PHONY: test
test: test.lint test.unit

.PHONY: test.lint
test.lint:
	@if [ -n "$(GOFMT_OUTPUT)" ]; then \
		echo "$(GOFMT_OUTPUT)"; \
		exit 1; \
	fi

.PHONY: test.unit
test.unit:
	go test -v ./...
