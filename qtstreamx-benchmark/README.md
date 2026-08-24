# qtstreamx-benchmark

JMH benchmarks for Binance JSON market-data parsing.

`JsonParserBenchmark` compares the QTStreamX/Jackson path with simdjson-java,
fastjson2, and Gson against recorded Binance combined-stream and kline frames.
The benchmark is a measurement harness, not a production parser dependency.

It uses three warm-up iterations, five measured iterations, two forks, and the
JDK Vector incubator module. Results are machine- and JVM-specific; compare
changes on the same environment.

## Run

```bash
gradle :qtstreamx-benchmark:jmh
```

Use JDK 25 with the project's configured Vector-module arguments. Run the
normal unit suite separately when changing benchmark fixtures or adapters.
