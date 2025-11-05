# exlo
Opinionated extract and load framework

## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

### Testing

**Unit and Integration Tests:**
```bash
sbt exlo/test
```
Runs all standard tests (31 tests, ~25 seconds).

**Performance Tests:**
```bash
sbt exlo/perf:test
```
Runs performance/throughput benchmarks (3 tests, ~30 seconds). These tests:
- Measure throughput with 10K, 100K, and 50K records
- Verify memory efficiency under load
- Compare different checkpoint interval strategies
- Use real Docker containers (Nessie + MinIO)

Performance tests are in `exlo/src/perf` and run separately to keep regular test runs fast.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).
