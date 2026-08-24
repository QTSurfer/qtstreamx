package com.qtsurfer.qtstreamx.dex.discovery.uniswap;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import com.qtsurfer.qtstreamx.dex.core.EvmToken;
import com.qtsurfer.qtstreamx.evm.rpc.EvmLogFilter;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcLog;
import com.qtsurfer.qtstreamx.evm.rpc.EvmRpcReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

abstract class AbstractUniswapMarketDiscovery<T> implements MarketDiscovery<T> {

    private final UniswapFactoryScan scan;
    private final EvmRpcReader reader;
    private final UniswapDiscoveryPolicy policy;
    private final UniswapDiscoveryListener listener;
    private final Erc20TokenCache tokenCache;
    private final String eventTopic;
    private final String swapTopic;
    private volatile Set<T> snapshot = Set.of();
    private Map<String, Candidate<T>> markets = Map.of();
    private long nextBlock;

    AbstractUniswapMarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapPairOrientation orientation,
            UniswapDiscoveryListener listener,
            String eventTopic,
            String swapTopic) {
        this(
                scan,
                reader,
                UniswapDiscoveryPolicy.compatibility(orientation),
                listener,
                eventTopic,
                swapTopic);
    }

    AbstractUniswapMarketDiscovery(
            UniswapFactoryScan scan,
            EvmRpcReader reader,
            UniswapDiscoveryPolicy policy,
            UniswapDiscoveryListener listener,
            String eventTopic,
            String swapTopic) {
        this.scan = Objects.requireNonNull(scan, "scan");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.tokenCache = new Erc20TokenCache(reader);
        this.eventTopic = eventTopic;
        this.swapTopic = swapTopic;
        this.nextBlock = scan.startBlock();
    }

    @Override
    public synchronized CompletionStage<Set<T>> refresh(long safeHead) {
        if (safeHead < 0 || safeHead == Long.MAX_VALUE) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("safeHead must allow a non-negative next block"));
        }
        if (safeHead < nextBlock) {
            return CompletableFuture.completedFuture(snapshot);
        }
        try {
            long scanBlocks = safeHead - nextBlock + 1;
            if (scanBlocks > policy.limits().maxScanBlocks()) {
                throw new UniswapDiscoveryException(
                        UniswapDiscoveryException.Kind.SCAN_RANGE_LIMIT);
            }
            List<EvmRpcLog> logs = reader.logs(
                    new EvmLogFilter(Set.of(scan.factoryAddress()), Set.of(eventTopic)),
                    nextBlock,
                    safeHead);
            Map<String, Candidate<T>> refreshed = new LinkedHashMap<>(markets);
            RefreshBudget budget = new RefreshBudget(policy.limits().maxMetadataCalls());
            for (EvmRpcLog log : logs) {
                add(refreshed, log, safeHead, budget);
            }
            Set<T> refreshedSnapshot = select(refreshed, safeHead);
            markets = refreshed;
            snapshot = refreshedSnapshot;
            nextBlock = safeHead + 1;
            return CompletableFuture.completedFuture(refreshedSnapshot);
        } catch (UniswapDiscoveryException exception) {
            return CompletableFuture.failedFuture(exception);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(new UniswapDiscoveryException());
        }
    }

    @Override
    public final Set<T> snapshot() {
        return snapshot;
    }

    @Override
    public final synchronized long nextBlock() {
        return nextBlock;
    }

    abstract UniswapFactoryEvent decode(EvmRpcLog log);

    abstract T create(
            UniswapFactoryEvent event,
            EvmToken token0,
            EvmToken token1,
            Instrument instrument);

    final UniswapFactoryScan scan() {
        return scan;
    }

    private void add(
            Map<String, Candidate<T>> refreshed,
            EvmRpcLog log,
            long safeHead,
            RefreshBudget budget) {
        if (log.removed()) {
            return;
        }
        if (!scan.factoryAddress().equalsIgnoreCase(log.address())) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.MALFORMED_EVENT);
            return;
        }
        UniswapFactoryEvent event;
        try {
            event = decode(log);
        } catch (RuntimeException exception) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.MALFORMED_EVENT);
            return;
        }
        if (refreshed.containsKey(event.marketAddress())) {
            return;
        }
        boolean acceptsAddresses;
        try {
            acceptsAddresses = policy.orientation().acceptsAddresses(
                    scan.network(), event.token0Address(), event.token1Address());
        } catch (RuntimeException exception) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.ORIENTATION);
            return;
        }
        if (!acceptsAddresses) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.ORIENTATION);
            return;
        }
        int requiredMetadataCalls = (tokenCache.contains(event.token0Address()) ? 0 : 2)
                + (tokenCache.contains(event.token1Address()) ? 0 : 2);
        budget.reserveMetadataCalls(requiredMetadataCalls);
        EvmToken token0;
        EvmToken token1;
        try {
            token0 = tokenCache.resolve(event.token0Address(), safeHead);
            token1 = tokenCache.resolve(event.token1Address(), safeHead);
        } catch (RuntimeException exception) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.TOKEN_METADATA);
            return;
        }
        Optional<Instrument> instrument;
        try {
            instrument = Objects.requireNonNull(
                    policy.orientation().orient(scan.network(), token0, token1),
                    "orientation result");
        } catch (RuntimeException exception) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.ORIENTATION);
            return;
        }
        if (instrument.isEmpty()) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.ORIENTATION);
            return;
        }
        T descriptor;
        try {
            descriptor = create(event, token0, token1, instrument.orElseThrow());
        } catch (RuntimeException exception) {
            notifyFailure(log, UniswapDiscoveryFailure.Kind.INVALID_DESCRIPTOR);
            return;
        }
        if (refreshed.size() >= policy.limits().maxDiscoveredMarkets()) {
            throw new UniswapDiscoveryException(
                    UniswapDiscoveryException.Kind.DISCOVERED_MARKET_LIMIT);
        }
        refreshed.put(
                event.marketAddress(),
                new Candidate<>(descriptor, log.blockNumber(), log.transactionHash()));
    }

    private Set<T> select(Map<String, Candidate<T>> candidates, long safeHead) {
        Set<T> selected = new LinkedHashSet<>();
        for (Map.Entry<String, Candidate<T>> entry : candidates.entrySet()) {
            Candidate<T> candidate = entry.getValue();
            if (!isActive(entry.getKey(), safeHead)) {
                notifyFailure(
                        candidate.blockNumber(),
                        candidate.transactionHash(),
                        UniswapDiscoveryFailure.Kind.INACTIVE_MARKET);
                continue;
            }
            if (selected.size() >= policy.limits().maxOutputMarkets()) {
                throw new UniswapDiscoveryException(
                        UniswapDiscoveryException.Kind.OUTPUT_MARKET_LIMIT);
            }
            selected.add(candidate.descriptor());
        }
        return Collections.unmodifiableSet(selected);
    }

    private boolean isActive(String marketAddress, long safeHead) {
        if (policy.activityLookbackBlocks().isEmpty()) {
            return true;
        }
        long lookback = policy.activityLookbackBlocks().orElseThrow();
        long fromBlock = safeHead >= lookback - 1 ? safeHead - lookback + 1 : 0;
        return reader.logs(
                        new EvmLogFilter(Set.of(marketAddress), Set.of(swapTopic)),
                        fromBlock,
                        safeHead).stream()
                .anyMatch(log -> !log.removed()
                        && marketAddress.equalsIgnoreCase(log.address())
                        && !log.topics().isEmpty()
                        && swapTopic.equalsIgnoreCase(log.topics().getFirst()));
    }

    private void notifyFailure(EvmRpcLog log, UniswapDiscoveryFailure.Kind kind) {
        notifyFailure(log.blockNumber(), log.transactionHash(), kind);
    }

    private void notifyFailure(
            long blockNumber,
            String transactionHash,
            UniswapDiscoveryFailure.Kind kind) {
        try {
            listener.onFailure(new UniswapDiscoveryFailure(
                    kind, blockNumber, transactionHash));
        } catch (RuntimeException ignored) {
            // Diagnostics must not make an otherwise valid refresh fail.
        }
    }

    private static final class RefreshBudget {

        private final int maxMetadataCalls;
        private int metadataCalls;

        private RefreshBudget(int maxMetadataCalls) {
            this.maxMetadataCalls = maxMetadataCalls;
        }

        private void reserveMetadataCalls(int calls) {
            if (calls > maxMetadataCalls - metadataCalls) {
                throw new UniswapDiscoveryException(
                        UniswapDiscoveryException.Kind.METADATA_CALL_LIMIT);
            }
            metadataCalls += calls;
        }
    }

    private record Candidate<T>(T descriptor, long blockNumber, String transactionHash) {}
}
