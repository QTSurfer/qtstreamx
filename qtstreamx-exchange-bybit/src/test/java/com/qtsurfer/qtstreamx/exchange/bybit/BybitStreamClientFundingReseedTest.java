package com.qtsurfer.qtstreamx.exchange.bybit;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.client.StreamClientConfig;
import com.qtsurfer.qtstreamx.core.model.FundingRate;
import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the funding REST re-seed: seeding the funding cache from the REST tickers endpoint lets a
 * bare tickers DELTA (which omits {@code fundingRate}) emit funding even when the WS SNAPSHOT was
 * never seen — closing the funding gap where a perp whose snapshot was missed under backpressure
 * stayed permanently dark (it had left roughly half the perpetual universe without funding).
 */
class BybitStreamClientFundingReseedTest {

  private static final String TICKERS_JSON =
      """
      {"retCode":0,"result":{"list":[
        {"symbol":"ETHUSDT","fundingRate":"0.0001","nextFundingTime":"1781740800000","markPrice":"2500.5"},
        {"symbol":"BTCUSDT","fundingRate":"0.00005","nextFundingTime":"1781740800000","markPrice":"65000"}
      ]}}
      """;

  private static final Instrument ETH = new Instrument("ETH", "USDT", "USDT"); // ETHUSDT linear perp

  /** A bare tickers delta omitting fundingRate — exactly what bybit sends after the snapshot. */
  private static final String ETH_DELTA =
      "{\"topic\":\"tickers.ETHUSDT\",\"ts\":1781716915358,"
          + "\"data\":{\"symbol\":\"ETHUSDT\",\"markPrice\":\"2501.0\"}}";

  private static BybitStreamClient linearClient() {
    return new BybitStreamClient(
        StreamClientConfig.withDefaults(() -> null),
        BybitStreamClient.Category.LINEAR,
        () -> TICKERS_JSON);
  }

  @Test
  void restReseedLetsADeltaEmitFundingWithoutAWsSnapshot() {
    BybitStreamClient client = linearClient();
    List<FundingRate> emitted = new ArrayList<>();
    client.subscribeFundingRate(ETH, emitted::add);

    client.reseedFundingFromRest(); // on-connect / periodic seed; no WS snapshot has arrived
    client.handleMessage(ETH_DELTA); // bare delta omitting fundingRate

    assertThat(emitted).hasSize(1);
    assertThat(emitted.get(0).instrument()).isEqualTo(ETH);
    assertThat(emitted.get(0).rate()).isEqualByComparingTo("0.0001"); // seeded from REST
  }

  @Test
  void withoutReseedABareDeltaEmitsNothing() {
    BybitStreamClient client = linearClient();
    List<FundingRate> emitted = new ArrayList<>();
    client.subscribeFundingRate(ETH, emitted::add);

    client.handleMessage(ETH_DELTA); // no reseed, no prior snapshot → cache.rate is null

    assertThat(emitted).isEmpty(); // this is exactly the funding gap
  }

  @Test
  void reseedOnlySeedsSubscribedTopics() {
    BybitStreamClient client = linearClient();
    List<FundingRate> emitted = new ArrayList<>();
    client.subscribeFundingRate(ETH, emitted::add); // only ETH; BTC is in the REST list but unsubscribed

    client.reseedFundingFromRest();
    client.handleMessage(
        "{\"topic\":\"tickers.BTCUSDT\",\"ts\":1,\"data\":{\"symbol\":\"BTCUSDT\",\"markPrice\":\"1\"}}");
    client.handleMessage(ETH_DELTA);

    assertThat(emitted).hasSize(1);
    assertThat(emitted.get(0).instrument()).isEqualTo(ETH);
  }
}
