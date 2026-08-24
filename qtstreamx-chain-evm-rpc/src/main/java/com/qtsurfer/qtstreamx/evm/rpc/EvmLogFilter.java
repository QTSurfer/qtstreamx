package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Set;

/**
 * Contract and event-topic selection for an EVM log stream.
 *
 * @param addresses emitting contract addresses
 * @param eventTopics accepted first event topics
 */
public record EvmLogFilter(Set<String> addresses, Set<String> eventTopics) {
    /** Creates an immutable filter. */
    public EvmLogFilter {
        addresses = Set.copyOf(addresses);
        eventTopics = Set.copyOf(eventTopics);
    }
}
