package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class EvmRpcProbeBudgetTest {
    @Test
    void exposesConservativeFiniteDefaults() {
        EvmRpcProbeBudget budget = EvmRpcProbeBudget.safeDefaults();

        assertThat(budget.maxRequests()).isEqualTo(12);
        assertThat(budget.maxWallClock()).isEqualTo(Duration.ofSeconds(45));
        assertThat(budget.maxLogBlockRange()).isEqualTo(10_000);
        assertThat(budget.maxReturnedLogs()).isEqualTo(10_000);
    }

    @Test
    void rejectsUnboundedOrNegativeCeilings() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvmRpcProbeBudget(
                        0,
                        Duration.ofSeconds(1),
                        1,
                        1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvmRpcProbeBudget(
                        1,
                        Duration.ZERO,
                        1,
                        1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvmRpcProbeBudget(
                        1,
                        Duration.ofSeconds(1),
                        0,
                        1));
    }
}
