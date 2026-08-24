package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.Objects;

/**
 * Deterministic HTTP probe inputs selected by an operator for one EVM network.
 *
 * <p>The recent and historical log intervals are independent from the exact historical state
 * block. Successful old logs therefore cannot silently stand in for historical state support.
 *
 * @param logFilter address/topic filter known to be valid on the configured network
 * @param recentLogsFromBlock first recent recovery block, inclusive
 * @param recentLogsToBlock last recent recovery block, inclusive
 * @param historicalLogsFromBlock first old discovery block, inclusive
 * @param historicalLogsToBlock last old discovery block, inclusive
 * @param stateContractAddress contract used for current and historical state probes
 * @param callData read-only ABI input used for current and historical {@code eth_call}
 * @param historicalStateBlock exact old block used for call and bytecode probes
 */
public record EvmRpcProbePlan(
        EvmLogFilter logFilter,
        long recentLogsFromBlock,
        long recentLogsToBlock,
        long historicalLogsFromBlock,
        long historicalLogsToBlock,
        String stateContractAddress,
        byte[] callData,
        long historicalStateBlock
) {
    private static final String EVM_ADDRESS = "0x[0-9a-fA-F]{40}";
    private static final String EVM_TOPIC = "0x[0-9a-fA-F]{64}";

    /** Validates and defensively copies the deterministic probe plan. */
    public EvmRpcProbePlan {
        Objects.requireNonNull(logFilter, "logFilter");
        Objects.requireNonNull(stateContractAddress, "stateContractAddress");
        Objects.requireNonNull(callData, "callData");
        if (logFilter.addresses().size() != 1
                || logFilter.addresses().stream().noneMatch(address -> address.matches(EVM_ADDRESS))) {
            throw new IllegalArgumentException("logFilter must contain one 20-byte address");
        }
        if (logFilter.eventTopics().size() != 1
                || logFilter.eventTopics().stream().noneMatch(topic -> topic.matches(EVM_TOPIC))) {
            throw new IllegalArgumentException("logFilter must contain one 32-byte event topic");
        }
        validateRange(recentLogsFromBlock, recentLogsToBlock, "recent log interval");
        validateRange(historicalLogsFromBlock, historicalLogsToBlock, "historical log interval");
        if (historicalStateBlock < 0) {
            throw new IllegalArgumentException("historicalStateBlock must be non-negative");
        }
        if (!stateContractAddress.matches(EVM_ADDRESS)) {
            throw new IllegalArgumentException("stateContractAddress must be a 20-byte hex value");
        }
        callData = callData.clone();
    }

    /**
     * Returns a defensive copy of the ABI input.
     *
     * @return new byte array containing the configured call data
     */
    @Override
    public byte[] callData() {
        return callData.clone();
    }

    private static void validateRange(long fromBlock, long toBlock, String label) {
        if (fromBlock < 0 || toBlock < fromBlock) {
            throw new IllegalArgumentException(label + " must be non-negative and ordered");
        }
    }
}
