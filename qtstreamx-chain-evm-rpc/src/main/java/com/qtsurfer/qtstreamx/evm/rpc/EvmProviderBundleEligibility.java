package com.qtsurfer.qtstreamx.evm.rpc;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/** Applies purpose-specific capability and canonical-head gates to runtime bundles. */
public final class EvmProviderBundleEligibility {
    private static final Set<EvmRpcProbePurpose> REQUIRED_LIVE_CAPABILITIES = EnumSet.of(
            EvmRpcProbePurpose.NETWORK,
            EvmRpcProbePurpose.HEAD,
            EvmRpcProbePurpose.FINALITY,
            EvmRpcProbePurpose.LIVE_STATE,
            EvmRpcProbePurpose.RECOVERY_LOGS,
            EvmRpcProbePurpose.LIVE_SUBSCRIPTION);
    private static final Set<EvmRpcProbePurpose> REQUIRED_DISCOVERY_CAPABILITIES = EnumSet.of(
            EvmRpcProbePurpose.NETWORK,
            EvmRpcProbePurpose.HEAD,
            EvmRpcProbePurpose.FINALITY,
            EvmRpcProbePurpose.DISCOVERY_LOGS,
            EvmRpcProbePurpose.HISTORICAL_STATE);

    private EvmProviderBundleEligibility() {}

    public static void requireLive(
            List<EvmProviderBundle> bundles,
            String network,
            long maximumProviderLagBlocks) {
        for (EvmProviderBundle bundle : bundles) {
            requireCapabilities(bundle, network, REQUIRED_LIVE_CAPABILITIES);
            requireTransportAgreement(bundle, maximumProviderLagBlocks);
        }
        requireCanonicalAgreement(bundles, maximumProviderLagBlocks);
    }

    public static EvmProviderBundle selectDiscovery(
            List<EvmProviderBundle> bundles,
            String network,
            long maximumProviderLagBlocks) {
        requireCanonicalAgreement(bundles, maximumProviderLagBlocks);
        return bundles.stream()
                .filter(bundle -> supports(bundle, network, REQUIRED_DISCOVERY_CAPABILITIES))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "no provider has purpose-specific discovery capability evidence"));
    }

    private static void requireCapabilities(
            EvmProviderBundle bundle,
            String network,
            Set<EvmRpcProbePurpose> purposes) {
        if (!network.equals(bundle.capabilities().network())) {
            throw new IllegalArgumentException(
                    "provider " + bundle.upstreamId() + " has capability evidence for another network");
        }
        for (EvmRpcProbePurpose purpose : purposes) {
            if (!bundle.capabilities().supports(purpose)) {
                throw new IllegalArgumentException(
                        "provider " + bundle.upstreamId() + " lacks " + purpose + " capability evidence");
            }
        }
    }

    private static boolean supports(
            EvmProviderBundle bundle,
            String network,
            Set<EvmRpcProbePurpose> purposes) {
        return network.equals(bundle.capabilities().network())
                && purposes.stream().allMatch(bundle.capabilities()::supports);
    }

    private static void requireTransportAgreement(
            EvmProviderBundle bundle,
            long maximumProviderLagBlocks) {
        boolean httpNetwork = supportsNetwork(bundle.capabilities(), EvmRpcTransport.HTTP);
        boolean webSocketNetwork = supportsNetwork(
                bundle.capabilities(), EvmRpcTransport.WEBSOCKET);
        Head httpHead = safeHead(bundle.capabilities(), EvmRpcTransport.HTTP)
                .orElseThrow(() -> new IllegalArgumentException(
                        "provider " + bundle.upstreamId() + " lacks HTTP safe-head evidence"));
        Head webSocketHead = safeHead(bundle.capabilities(), EvmRpcTransport.WEBSOCKET)
                .orElseThrow(() -> new IllegalArgumentException(
                        "provider " + bundle.upstreamId() + " lacks WebSocket safe-head evidence"));
        if (!httpNetwork || !webSocketNetwork) {
            throw new IllegalArgumentException(
                    "provider " + bundle.upstreamId() + " lacks HTTP/WebSocket network evidence");
        }
        long lag = httpHead.number() >= webSocketHead.number()
                ? httpHead.number() - webSocketHead.number()
                : webSocketHead.number() - httpHead.number();
        if (lag > maximumProviderLagBlocks) {
            throw new IllegalArgumentException(
                    "provider " + bundle.upstreamId() + " HTTP/WebSocket safe-head lag exceeds limit");
        }
        if (lag == 0 && !httpHead.hash().equals(webSocketHead.hash())) {
            throw new IllegalArgumentException(
                    "provider " + bundle.upstreamId() + " HTTP/WebSocket safe-head hashes diverge");
        }
    }

    private static boolean supportsNetwork(
            EvmRpcCapabilityReport report,
            EvmRpcTransport transport) {
        return report.observations().stream()
                .filter(observation -> observation.transport() == transport)
                .filter(observation -> observation.purpose() == EvmRpcProbePurpose.NETWORK)
                .anyMatch(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED);
    }

    private static Optional<Head> safeHead(
            EvmRpcCapabilityReport report,
            EvmRpcTransport transport) {
        return report.observations().stream()
                .filter(observation -> observation.transport() == transport)
                .filter(observation -> observation.operation() == EvmRpcProbeOperation.SAFE_BLOCK)
                .filter(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED)
                .filter(observation -> observation.fromBlock().isPresent())
                .filter(observation -> observation.blockHash() != null
                        && !observation.blockHash().isBlank())
                .map(observation -> new Head(
                        observation.fromBlock().getAsLong(), observation.blockHash()))
                .reduce((first, second) -> second);
    }

    public static long safeHeadNumber(EvmProviderBundle bundle) {
        return safeHead(bundle.capabilities(), EvmRpcTransport.HTTP)
                .orElseThrow(() -> new IllegalArgumentException(
                        "provider " + bundle.upstreamId() + " lacks HTTP safe-head evidence"))
                .number();
    }

    private static void requireCanonicalAgreement(
            List<EvmProviderBundle> bundles,
            long maximumProviderLagBlocks) {
        if (bundles.size() != 2) {
            throw new IllegalArgumentException("active/passive routing requires exactly two bundles");
        }
        EvmProviderBundle active = bundles.get(0);
        EvmProviderBundle passive = bundles.get(1);
        Instant measuredAt = active.capabilities().finishedAt().isAfter(passive.capabilities().finishedAt())
                ? active.capabilities().finishedAt()
                : passive.capabilities().finishedAt();
        Instant startedAt = active.capabilities().startedAt().isBefore(passive.capabilities().startedAt())
                ? active.capabilities().startedAt()
                : passive.capabilities().startedAt();
        EvmRpcProviderRelation relation = EvmRpcProviderComparison.compare(
                active.capabilities(),
                passive.capabilities(),
                EvmRpcProbeOperation.SAFE_BLOCK,
                maximumProviderLagBlocks,
                measuredAt,
                Duration.between(startedAt, measuredAt));
        Instant oldestAccepted = startedAt;
        OptionalLong activeHead = safeHead(active.capabilities(), oldestAccepted, measuredAt);
        OptionalLong passiveHead = safeHead(passive.capabilities(), oldestAccepted, measuredAt);
        boolean boundedSequentialSkew = relation == EvmRpcProviderRelation.UNKNOWN
                && activeHead.isPresent()
                && passiveHead.isPresent()
                && activeHead.getAsLong() != passiveHead.getAsLong();
        if (relation != EvmRpcProviderRelation.CONSISTENT && !boundedSequentialSkew) {
            throw new IllegalArgumentException("provider capability relation is " + relation);
        }
    }

    private static OptionalLong safeHead(
            EvmRpcCapabilityReport report,
            Instant oldestAccepted,
            Instant measuredAt) {
        return report.observations().stream()
                .filter(observation -> observation.operation() == EvmRpcProbeOperation.SAFE_BLOCK)
                .filter(observation -> observation.status() == EvmRpcProbeStatus.SUPPORTED)
                .filter(observation -> observation.fromBlock().isPresent())
                .filter(observation -> observation.blockHash() != null
                        && !observation.blockHash().isBlank())
                .filter(observation -> !observation.measuredAt().isBefore(oldestAccepted))
                .filter(observation -> !observation.measuredAt().isAfter(measuredAt))
                .mapToLong(observation -> observation.fromBlock().getAsLong())
                .reduce((first, second) -> second);
    }

    private record Head(long number, String hash) {}
}
