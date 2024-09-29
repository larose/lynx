.PHONY: index
index:
	time go run . -mode=index

.PHONY: search
search:
	time go run . -mode=search


.PHONY: index.cpu.profile
index.cpu.profile:
	go tool pprof -http=":8888" index.cpu.pprof

.PHONY: search.cpu.profile
search.cpu.profile:
	go tool pprof -http=":8888" search.cpu.pprof
