package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Selects one active EVM provider bundle and replaces it once with a passive bundle. */
public final class ActivePassiveEvmLogStream implements EvmLogStream {
    private final List<EvmProviderBundle> bundles;
    private final EvmLogStreamId streamId;
    private final EvmLogCheckpointStore checkpointStore;
    private final int overlapBlocks;
    private final long maxReplayBlocks;
    private final EvmLogStreamFactory factory;
    private final long maximumSafeHead;
    private final ActivePassiveEvmSupervisorMetrics metrics =
            new ActivePassiveEvmSupervisorMetrics();
    private final AtomicBoolean started = new AtomicBoolean();

    private Consumer<Throwable> errorHandler = ignored -> {};
    private EvmLogStream current;
    private int currentIndex;
    private boolean switching;
    private boolean closed;

    public ActivePassiveEvmLogStream(
            List<EvmProviderBundle> bundles,
            EvmLogStreamId streamId,
            EvmLogCheckpointStore checkpointStore,
            int overlapBlocks,
            long maxReplayBlocks,
            EvmLogStreamFactory factory) {
        this(
                bundles,
                streamId,
                checkpointStore,
                overlapBlocks,
                maxReplayBlocks,
                0,
                factory);
    }

    public ActivePassiveEvmLogStream(
            List<EvmProviderBundle> bundles,
            EvmLogStreamId streamId,
            EvmLogCheckpointStore checkpointStore,
            int overlapBlocks,
            long maxReplayBlocks,
            long maximumProviderLagBlocks,
            EvmLogStreamFactory factory) {
        Objects.requireNonNull(bundles, "bundles");
        if (bundles.size() != 2) {
            throw new IllegalArgumentException("active/passive supervision requires exactly two bundles");
        }
        this.bundles = List.copyOf(bundles);
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        if (maximumProviderLagBlocks < 0) {
            throw new IllegalArgumentException("maximumProviderLagBlocks must be non-negative");
        }
        EvmProviderBundleEligibility.requireLive(
                this.bundles, streamId.network(), maximumProviderLagBlocks);
        maximumSafeHead = this.bundles.stream()
                .mapToLong(EvmProviderBundleEligibility::safeHeadNumber)
                .max()
                .orElseThrow();
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
        if (overlapBlocks < 0) {
            throw new IllegalArgumentException("overlapBlocks must be non-negative");
        }
        if (maxReplayBlocks < 1) {
            throw new IllegalArgumentException("maxReplayBlocks must be positive");
        }
        this.overlapBlocks = overlapBlocks;
        this.maxReplayBlocks = maxReplayBlocks;
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    @Override
    public void start(EvmLogFilter filter, Consumer<EvmLog> handler) {
        throw new UnsupportedOperationException("Active/passive supervision requires durable recovery");
    }

    @Override
    public synchronized void startRecoverable(EvmLogFilter filter, EvmLogBatchHandler handler)
            throws Exception {
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(handler, "handler");
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("supervised stream has already started");
        }
        startCurrent(filter, handler);
    }

    @Override
    public synchronized void onError(Consumer<Throwable> handler) {
        errorHandler = Objects.requireNonNull(handler, "handler");
    }

    @Override
    public synchronized boolean isConnected() {
        return current != null && current.isConnected();
    }

    @Override
    public synchronized EvmLogStreamStatus status() {
        if (closed) {
            return EvmLogStreamStatus.CLOSED;
        }
        return current == null ? EvmLogStreamStatus.IDLE : current.status();
    }

    public ActivePassiveEvmSupervisorMetrics.Snapshot metrics() {
        EvmLogStream selected;
        synchronized (this) {
            selected = current;
        }
        return metrics.snapshot(selected == null
                ? EvmLogStreamMetrics.empty()
                : selected.recoveryMetrics());
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (current != null) {
            current.close();
        }
    }

    private void startCurrent(EvmLogFilter filter, EvmLogBatchHandler handler) throws Exception {
        while (true) {
            EvmProviderBundle bundle = bundles.get(currentIndex);
            EvmRecoveryPolicy policy = new EvmRecoveryPolicy(
                    streamId,
                    bundle.upstreamId(),
                    overlapBlocks,
                    maxReplayBlocks);
            current = Objects.requireNonNull(
                    factory.create(bundle, checkpointStore, policy),
                    "factory result");
            metrics.selected(
                    bundle.upstreamId(),
                    maximumSafeHead - EvmProviderBundleEligibility.safeHeadNumber(bundle));
            int selectedIndex = currentIndex;
            current.onError(error -> handleAsynchronousFailure(selectedIndex, filter, handler, error));
            switching = true;
            try {
                current.startRecoverable(filter, handler);
                return;
            } catch (Exception exception) {
                closeCurrent(exception);
                if (!canSwitch(exception)) {
                    metrics.terminal(exception);
                    throw exception;
                }
                metrics.switched();
                currentIndex++;
            } finally {
                switching = false;
            }
        }
    }

    private synchronized void handleAsynchronousFailure(
            int selectedIndex,
            EvmLogFilter filter,
            EvmLogBatchHandler handler,
            Throwable failure) {
        if (closed || switching || selectedIndex != currentIndex) {
            return;
        }
        if (!canSwitch(failure)) {
            metrics.terminal(failure);
            errorHandler.accept(failure);
            return;
        }
        try {
            closeCurrent(failure);
            metrics.switched();
            currentIndex++;
            startCurrent(filter, handler);
        } catch (Exception exception) {
            errorHandler.accept(exception);
        }
    }

    private boolean canSwitch(Throwable failure) {
        return currentIndex + 1 < bundles.size()
                && failure instanceof EvmRecoveryTransitionException transition
                && transition.stage() == EvmRecoveryTransitionException.Stage.UPSTREAM_RECOVERY;
    }

    private void closeCurrent(Throwable failure) {
        EvmLogStream closing = current;
        try {
            closing.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        } finally {
            metrics.retired(closing.recoveryMetrics());
            current = null;
        }
    }
}
