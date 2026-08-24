package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;

interface EvmRpcProbeHttpClient {
    long chainId();

    long latestBlockNumber();

    EvmBlock getBlock(EvmBlockTag blockTag);

    List<EvmRpcLog> getLogs(
            EvmLogFilter filter,
            long fromBlock,
            long toBlock,
            int maxResults);

    byte[] call(String contractAddress, byte[] data, EvmBlockTag blockTag);

    byte[] code(String contractAddress, EvmBlockTag blockTag);
}
