package com.qtsurfer.qtstreamx.core.client;

import com.qtsurfer.qtstreamx.core.model.MarketTrade;
import java.util.List;
import java.util.Objects;

/**
 * Immutable normalized trades offered at one source acknowledgement boundary.
 *
 * @param trades ordered normalized trades; may be empty
 */
public record MarketTradeBatch(List<MarketTrade> trades) {
    /** Validates and snapshots the trade batch. */
    public MarketTradeBatch {
        Objects.requireNonNull(trades, "trades");
        trades = List.copyOf(trades);
    }
}
