package com.qtsurfer.qtstreamx.codec.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qtsurfer.qtstreamx.core.codec.StreamCodec;
import com.qtsurfer.qtstreamx.core.model.Instrument;

/**
 * JSON codec using Jackson. Reference implementation — readable, debuggable,
 * but not the fastest option for high-throughput ingestion.
 */
public class JsonCodec<T> implements StreamCodec<T> {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().addMixIn(Instrument.class, InstrumentJsonMixin.class);

    @Override
    public byte[] encode(T value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new CodecException("JSON encode failed", e);
        }
    }

    @Override
    public T decode(byte[] data, Class<T> type) {
        try {
            return MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new CodecException("JSON decode failed", e);
        }
    }

    @Override
    public String name() {
        return "json";
    }

    public static class CodecException extends RuntimeException {
        public CodecException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @JsonIgnoreProperties(value = "derivative", allowGetters = true)
    private abstract static class InstrumentJsonMixin {}
}
