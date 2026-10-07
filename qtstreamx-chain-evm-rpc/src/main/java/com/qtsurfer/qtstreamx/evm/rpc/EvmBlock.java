package com.qtsurfer.qtstreamx.evm.rpc;

/** Canonical EVM block identity and timestamp in epoch microseconds. */
public record EvmBlock(long number, String hash, long timestamp) {}
