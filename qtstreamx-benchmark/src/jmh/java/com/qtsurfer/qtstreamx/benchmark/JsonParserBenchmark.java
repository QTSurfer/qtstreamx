package com.qtsurfer.qtstreamx.benchmark;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.openjdk.jmh.annotations.*;
import org.simdjson.JsonValue;
import org.simdjson.SimdJsonParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing JSON parsers for Binance WebSocket messages.
 *
 * <p>Parsers: Jackson, simdjson-java, fastjson2, Gson
 * <p>Messages: bookTicker (~120 bytes), kline (~300 bytes)
 * <p>Run: ./gradlew :qtstreamx-benchmark:jmh
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class JsonParserBenchmark {

    private ObjectMapper jackson;
    private SimdJsonParser simdJson;
    private byte[] bookTickerBytes;
    private byte[] klineBytes;
    private String bookTickerStr;
    private String klineStr;

    @Setup
    public void setup() throws IOException {
        jackson = new ObjectMapper();
        simdJson = new SimdJsonParser();
        bookTickerBytes = loadResource("binance-combined-stream.json");
        klineBytes = loadResource("binance-kline-stream.json");
        bookTickerStr = new String(bookTickerBytes);
        klineStr = new String(klineBytes);
    }

    // ==================== bookTicker full (5 fields) ====================

    @Benchmark
    public Object jackson_bookTicker() throws Exception {
        JsonNode data = jackson.readTree(bookTickerBytes).get("data");
        return new Object[]{
                data.get("s").asText(),
                new BigDecimal(data.get("b").asText()),
                new BigDecimal(data.get("B").asText()),
                new BigDecimal(data.get("a").asText()),
                new BigDecimal(data.get("A").asText())
        };
    }

    @Benchmark
    public Object simdjson_bookTicker() {
        JsonValue data = simdJson.parse(bookTickerBytes, bookTickerBytes.length).get("data");
        return new Object[]{
                data.get("s").asString(),
                new BigDecimal(data.get("b").asString()),
                new BigDecimal(data.get("B").asString()),
                new BigDecimal(data.get("a").asString()),
                new BigDecimal(data.get("A").asString())
        };
    }

    @Benchmark
    public Object fastjson2_bookTicker() {
        JSONObject data = JSON.parseObject(bookTickerStr).getJSONObject("data");
        return new Object[]{
                data.getString("s"),
                data.getBigDecimal("b"),
                data.getBigDecimal("B"),
                data.getBigDecimal("a"),
                data.getBigDecimal("A")
        };
    }

    @Benchmark
    public Object gson_bookTicker() {
        var data = JsonParser.parseString(bookTickerStr).getAsJsonObject().getAsJsonObject("data");
        return new Object[]{
                data.get("s").getAsString(),
                data.get("b").getAsBigDecimal(),
                data.get("B").getAsBigDecimal(),
                data.get("a").getAsBigDecimal(),
                data.get("A").getAsBigDecimal()
        };
    }

    // ==================== kline full (9 fields) ====================

    @Benchmark
    public Object jackson_kline() throws Exception {
        JsonNode k = jackson.readTree(klineBytes).get("data").get("k");
        return new Object[]{
                k.get("s").asText(), k.get("i").asText(),
                new BigDecimal(k.get("o").asText()), new BigDecimal(k.get("h").asText()),
                new BigDecimal(k.get("l").asText()), new BigDecimal(k.get("c").asText()),
                new BigDecimal(k.get("v").asText()), k.get("x").asBoolean(), k.get("t").asLong()
        };
    }

    @Benchmark
    public Object simdjson_kline() {
        JsonValue k = simdJson.parse(klineBytes, klineBytes.length).get("data").get("k");
        return new Object[]{
                k.get("s").asString(), k.get("i").asString(),
                new BigDecimal(k.get("o").asString()), new BigDecimal(k.get("h").asString()),
                new BigDecimal(k.get("l").asString()), new BigDecimal(k.get("c").asString()),
                new BigDecimal(k.get("v").asString()), k.get("x").asBoolean(), k.get("t").asLong()
        };
    }

    @Benchmark
    public Object fastjson2_kline() {
        JSONObject k = JSON.parseObject(klineStr).getJSONObject("data").getJSONObject("k");
        return new Object[]{
                k.getString("s"), k.getString("i"),
                k.getBigDecimal("o"), k.getBigDecimal("h"),
                k.getBigDecimal("l"), k.getBigDecimal("c"),
                k.getBigDecimal("v"), k.getBoolean("x"), k.getLong("t")
        };
    }

    @Benchmark
    public Object gson_kline() {
        var k = JsonParser.parseString(klineStr).getAsJsonObject()
                .getAsJsonObject("data").getAsJsonObject("k");
        return new Object[]{
                k.get("s").getAsString(), k.get("i").getAsString(),
                k.get("o").getAsBigDecimal(), k.get("h").getAsBigDecimal(),
                k.get("l").getAsBigDecimal(), k.get("c").getAsBigDecimal(),
                k.get("v").getAsBigDecimal(), k.get("x").getAsBoolean(), k.get("t").getAsLong()
        };
    }

    private byte[] loadResource(String name) throws IOException {
        try (var is = getClass().getClassLoader().getResourceAsStream(name)) {
            return is.readAllBytes();
        }
    }
}
