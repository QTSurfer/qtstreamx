package com.qtsurfer.qtstreamx.canary;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.canary.AnalyzeMain.PricePoint;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class AnalyzeMainTest {

    @Test
    void driftMatchesNearestNeighbour() {
        List<PricePoint> ref = List.of(
                new PricePoint(1_000_000L, bd("100")),
                new PricePoint(2_000_000L, bd("101")),
                new PricePoint(3_000_000L, bd("102")));
        List<PricePoint> tgt = List.of(
                new PricePoint(1_100_000L, bd("100.1")),   // 100us away
                new PricePoint(2_050_000L, bd("101.5")),   // 50us away
                new PricePoint(3_000_000L, bd("102")));    // exact match

        List<Double> drifts = AnalyzeMain.drift(ref, tgt, 200_000L);
        assertThat(drifts).hasSize(3);
        assertThat(drifts.get(0)).isCloseTo(0.001, offset(1e-6));     // 0.1 / 100
        assertThat(drifts.get(1)).isCloseTo(0.004950495, offset(1e-6)); // 0.5 / 101
        assertThat(drifts.get(2)).isEqualTo(0.0);
    }

    @Test
    void driftDropsPairsBeyondWindow() {
        List<PricePoint> ref = List.of(new PricePoint(1_000_000L, bd("100")));
        List<PricePoint> tgt = List.of(new PricePoint(5_000_000L, bd("100")));
        // 4s apart, window is 2s → no pair
        assertThat(AnalyzeMain.drift(ref, tgt, 2_000_000L)).isEmpty();
    }

    @Test
    void percentileIndexLinear() {
        // percentile uses floor(p * (n-1)) — no interpolation.
        List<Double> sorted = List.of(0.0, 0.1, 0.2, 0.3, 0.4);
        assertThat(AnalyzeMain.percentile(sorted, 0.5)).isEqualTo(0.2); // idx 2
        assertThat(AnalyzeMain.percentile(sorted, 0.95)).isEqualTo(0.3); // idx 3
        assertThat(AnalyzeMain.percentile(sorted, 1.0)).isEqualTo(0.4);  // idx 4
        assertThat(AnalyzeMain.percentile(List.of(), 0.5)).isEqualTo(0.0);
    }

    @Test
    void keyOfJoinsKindAndInstrument() {
        assertThat(AnalyzeMain.keyOf("ticker", "BTC/USDT")).isEqualTo("ticker|BTC/USDT");
    }

    private static BigDecimal bd(String s) { return new BigDecimal(s); }

    private static org.assertj.core.data.Offset<Double> offset(double o) {
        return org.assertj.core.data.Offset.offset(o);
    }
}
