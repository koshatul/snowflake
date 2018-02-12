-include artifacts/make/go/Makefile

run: artifacts/build/debug/$(GOOS)/$(GOARCH)/snowflake
	"$<" $(RUN_ARGS)

artifacts/make/%/Makefile:
	curl -sf https://jmalloc.github.io/makefiles/fetch | bash /dev/stdin $*
