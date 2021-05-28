.PHONY: all clean test fuzz bench-pack bench-layers bench doc examples

all:
	dune build

test:
	dune runtest

bench-layers:
	@dune exec -- ./bench/irmin-pack/layers.exe -n 2005 -b 2 -j

bench-with-trace-replay:
	@echo test 1>&2
	@mount 1>&2 || true
	@echo test 1>&2
	@ls -lRa /home/opam/bench-dir/bench-data 1>&2 || true
	@echo test 1>&2
	@ls -la 1>&2 || true
	@echo test 1>&2
	@dune exec -- ./bench/irmin-pack/tree.exe --mode trace test/irmin-bench/data/tezos_actions_1commit.repr --ncommits-trace 12000 --artefacts ./cb_artefacts 1>&2 || true
	@dune exec -- ./bench/irmin-pack/trace_stats.exe cb ./cb_artefacts/boostrap_summary.json
	@rm -rf ./cb_artefacts

bench: bench-with-trace-replay bench-layers

fuzz:
	dune build @fuzz --no-buffer

examples:
	dune build @examples

clean:
	dune clean

doc:
	dune build @doc
