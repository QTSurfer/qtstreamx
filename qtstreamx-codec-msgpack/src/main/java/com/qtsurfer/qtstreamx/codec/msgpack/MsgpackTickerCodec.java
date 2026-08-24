package com.qtsurfer.qtstreamx.codec.msgpack;

import com.qtsurfer.qtstreamx.core.codec.StreamCodec;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.core.model.Ticker;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Hand-tuned MessagePack codec for {@link Ticker}.
 *
 * <p>Encodes fields in fixed order as a MessagePack array for minimal overhead.
 * Nullable BigDecimal fields use nil. This is significantly faster than generic
 * reflection-based serialization.
 */
public class MsgpackTickerCodec implements StreamCodec<Ticker> {

    @Override
    public byte[] encode(Ticker t) {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packArrayHeader(12);
            packer.packString(t.instrument().symbol());
            packBigDecimal(packer, t.bid());
            packBigDecimal(packer, t.bidSize());
            packBigDecimal(packer, t.ask());
            packBigDecimal(packer, t.askSize());
            packBigDecimal(packer, t.last());
            packBigDecimal(packer, t.open());
            packBigDecimal(packer, t.high());
            packBigDecimal(packer, t.low());
            packBigDecimal(packer, t.volume());
            packBigDecimal(packer, t.quoteVolume());
            packer.packLong(t.timestamp());
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("MsgPack encode failed", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Ticker decode(byte[] data, Class<Ticker> type) {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            unpacker.unpackArrayHeader();
            Instrument instrument = Instrument.parse(unpacker.unpackString());
            BigDecimal bid = unpackBigDecimal(unpacker);
            BigDecimal bidSize = unpackBigDecimal(unpacker);
            BigDecimal ask = unpackBigDecimal(unpacker);
            BigDecimal askSize = unpackBigDecimal(unpacker);
            BigDecimal last = unpackBigDecimal(unpacker);
            BigDecimal open = unpackBigDecimal(unpacker);
            BigDecimal high = unpackBigDecimal(unpacker);
            BigDecimal low = unpackBigDecimal(unpacker);
            BigDecimal volume = unpackBigDecimal(unpacker);
            BigDecimal quoteVolume = unpackBigDecimal(unpacker);
            long timestamp = unpacker.unpackLong();
            return new Ticker(instrument, bid, bidSize, ask, askSize,
                    last, open, high, low, volume, quoteVolume, timestamp);
        } catch (IOException e) {
            throw new RuntimeException("MsgPack decode failed", e);
        }
    }

    @Override
    public String name() {
        return "msgpack";
    }

    private static void packBigDecimal(MessageBufferPacker packer, BigDecimal value) throws IOException {
        if (value == null) {
            packer.packNil();
        } else {
            packer.packString(value.toPlainString());
        }
    }

    private static BigDecimal unpackBigDecimal(MessageUnpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNil()) {
            return null;
        }
        return new BigDecimal(unpacker.unpackString());
    }
}
