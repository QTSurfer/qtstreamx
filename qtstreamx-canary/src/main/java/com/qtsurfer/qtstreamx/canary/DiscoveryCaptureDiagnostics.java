package com.qtsurfer.qtstreamx.canary;

import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryFailure;
import com.qtsurfer.qtstreamx.dex.discovery.uniswap.UniswapDiscoveryListener;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Counts stable discovery failure kinds without retaining provider-controlled data. */
final class DiscoveryCaptureDiagnostics implements UniswapDiscoveryListener {

    private final Map<UniswapDiscoveryFailure.Kind, Integer> reasons =
            new EnumMap<>(UniswapDiscoveryFailure.Kind.class);

    @Override
    public synchronized void onFailure(UniswapDiscoveryFailure failure) {
        reasons.merge(failure.kind(), 1, Integer::sum);
    }

    synchronized DiscoveryCaptureReport report(int selected) {
        Map<String, Integer> safeReasons = new LinkedHashMap<>();
        reasons.forEach((kind, count) -> safeReasons.put(kind.name(), count));
        int rejected = reasons.values().stream().mapToInt(Integer::intValue).sum();
        return new DiscoveryCaptureReport(selected + rejected, selected, rejected, safeReasons);
    }
}
