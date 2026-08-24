package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;

interface EvmRpcHttpClient {
    long latestBlockNumber();

    EvmBlock getBlock(long number);

    List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock);
}
