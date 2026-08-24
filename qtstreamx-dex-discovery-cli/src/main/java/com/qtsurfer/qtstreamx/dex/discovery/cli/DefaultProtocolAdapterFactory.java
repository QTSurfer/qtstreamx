package com.qtsurfer.qtstreamx.dex.discovery.cli;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapOnChainLookup;
import com.qtsurfer.qtstreamx.evm.rpc.EvmHttpRpcReader;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReaderConfig;
import java.time.Duration;

/** Wires the protocol adapters shipped in the CLI distribution. */
final class DefaultProtocolAdapterFactory implements ProtocolAdapterFactory {

    @Override
    public CliProtocolAdapter create(
            CliRequest.Protocol protocol,
            String network,
            String httpUrl) {
        if (protocol != CliRequest.Protocol.UNISWAP) {
            throw new IllegalArgumentException("protocol has no discovery adapter");
        }
        EvmRpcReaderConfig config = new EvmRpcReaderConfig(
                network,
                httpUrl,
                2_000,
                Duration.ofSeconds(10),
                3);
        try {
            return new OnChainUniswapCliAdapter(
                    network,
                    new UniswapOnChainLookup(new EvmHttpRpcReader(config)));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("RPC HTTP endpoint is invalid");
        }
    }
}
