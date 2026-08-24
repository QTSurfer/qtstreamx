package com.qtsurfer.qtstreamx.core.codec;

/**
 * Serialization/deserialization contract for stream messages.
 *
 * <p>Implementations: JSON (reference), MessagePack (performance), Protobuf (schema evolution).
 *
 * @param <T> the type to encode/decode
 */
public interface StreamCodec<T> {

    /** Encode a value to bytes. */
    byte[] encode(T value);

    /** Decode bytes to a value of the given type. */
    T decode(byte[] data, Class<T> type);

    /** Codec identifier (e.g. "json", "msgpack", "protobuf"). */
    String name();
}
