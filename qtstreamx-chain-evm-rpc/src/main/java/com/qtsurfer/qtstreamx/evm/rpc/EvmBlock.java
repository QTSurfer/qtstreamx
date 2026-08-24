package com.qtsurfer.qtstreamx.evm.rpc;

/** Canonical EVM block identity and Unix timestamp in seconds. */
public record EvmBlock(long number, String hash, long timestamp) {}
