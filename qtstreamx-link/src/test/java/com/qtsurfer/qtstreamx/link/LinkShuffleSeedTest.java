package com.qtsurfer.qtstreamx.link;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The link-shuffle seed exists so that redundant publisher instances bucket the same instrument
 * universe differently: a mute or dropped WS link then takes out a different subset on each
 * instance, and the chance of the same instrument being lost everywhere at once falls from ~p
 * towards ~p^N.
 *
 * <p>That guarantee is only as strong as the uniqueness of what feeds the seed. On the ingest fleet
 * it was measured to be absent: every node named its two publisher containers {@code pubx-1} and
 * {@code pubx-2}, so six processes across three nodes produced two groupings, not six.
 */
class LinkShuffleSeedTest {

  @Test
  void same_inputs_give_the_same_seed() {
    // Reconnects and restarts must not reshuffle which link owns which instrument.
    assertThat(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"))
        .isEqualTo(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"));
  }

  @Test
  void different_hostnames_give_different_seeds() {
    assertThat(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"))
        .isNotEqualTo(LinkManager.linkShuffleSeed("", "pubx-2", "bybit-linear"));
  }

  @Test
  void different_exchanges_give_different_seeds() {
    assertThat(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"))
        .isNotEqualTo(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-spot"));
  }

  @Test
  void the_salt_decorrelates_instances_that_share_a_hostname() {
    // This is the fix. Identical container name on every node — the fleet's actual deployment —
    // and the seeds must still differ.
    long ingS1 = LinkManager.linkShuffleSeed("ingS1", "pubx-1", "bybit-linear");
    long ingS2 = LinkManager.linkShuffleSeed("ingS2", "pubx-1", "bybit-linear");
    long ingS3 = LinkManager.linkShuffleSeed("ingS3", "pubx-1", "bybit-linear");

    assertThat(ingS1).isNotEqualTo(ingS2);
    assertThat(ingS2).isNotEqualTo(ingS3);
    assertThat(ingS1).isNotEqualTo(ingS3);
  }

  @Test
  void an_absent_salt_reproduces_the_pre_existing_seed() {
    // Regression guard: with no salt the value must be what the fleet has been running, so an
    // upgrade alone does not silently reshuffle every link's bucket. This constant was read from
    // ingS3's own startup log on 2026-07-28.
    assertThat(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"))
        .isEqualTo(-3379343098192013228L);
  }

  @Test
  void null_and_blank_salt_behave_alike() {
    assertThat(LinkManager.linkShuffleSeed(null, "pubx-1", "bybit-linear"))
        .isEqualTo(LinkManager.linkShuffleSeed("", "pubx-1", "bybit-linear"));
  }
}
