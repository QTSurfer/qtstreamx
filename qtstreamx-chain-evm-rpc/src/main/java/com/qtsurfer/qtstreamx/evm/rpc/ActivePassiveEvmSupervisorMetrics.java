package com.qtsurfer.qtstreamx.evm.rpc;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Collects endpoint-free active/passive selection and terminal counters. */
public final class ActivePassiveEvmSupervisorMetrics {
    private final Map<String, Long> selections = new LinkedHashMap<>();
    private final Map<ActivePassiveEvmTerminalReason, Long> terminalFailures =
            new EnumMap<>(ActivePassiveEvmTerminalReason.class);
    private String selectedUpstream = "none";
    private long headLagBlocks;
    private long switches;
    private long recoveryPages;
    private long retries;
    private long gaps;
    private long reorgs;
    private long duplicateSuppressions;
    private long streamTerminalFailures;

    synchronized void selected(String upstreamId, long headLagBlocks) {
        selectedUpstream = upstreamId;
        this.headLagBlocks = headLagBlocks;
        selections.merge(upstreamId, 1L, Long::sum);
    }

    synchronized void switched() {
        switches++;
    }

    synchronized void terminal(Throwable failure) {
        terminalFailures.merge(
                ActivePassiveEvmTerminalReason.classify(failure), 1L, Long::sum);
    }

    synchronized void retired(EvmLogStreamMetrics retired) {
        recoveryPages += retired.recoveryPages();
        retries += retired.retries();
        gaps += retired.gaps();
        reorgs += retired.reorgs();
        duplicateSuppressions += retired.duplicateSuppressions();
        streamTerminalFailures += retired.terminalFailures();
    }

    synchronized Snapshot snapshot(EvmLogStreamMetrics current) {
        return new Snapshot(
                selectedUpstream,
                switches,
                headLagBlocks,
                current.cursorLagBlocks(),
                recoveryPages + current.recoveryPages(),
                retries + current.retries(),
                gaps + current.gaps(),
                reorgs + current.reorgs(),
                duplicateSuppressions + current.duplicateSuppressions(),
                streamTerminalFailures + current.terminalFailures(),
                Map.copyOf(selections),
                Map.copyOf(terminalFailures));
    }

    /** Immutable, endpoint-free supervisor metrics. */
    public record Snapshot(
            String selectedUpstream,
            long switches,
            long headLagBlocks,
            long cursorLagBlocks,
            long recoveryPages,
            long retries,
            long gaps,
            long reorgs,
            long duplicateSuppressions,
            long streamTerminalFailures,
            Map<String, Long> selections,
            Map<ActivePassiveEvmTerminalReason, Long> terminalFailures
    ) {}
}
