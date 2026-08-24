package com.qtsurfer.qtstreamx.discovery.binance;

import static org.assertj.core.api.Assertions.assertThat;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BinanceInstrumentsCacheParseTest {

  private static final String SPOT_SAMPLE =
      """
      {
        "symbols": [
          {"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING"},
          {"symbol":"ETHUSDT","baseAsset":"ETH","quoteAsset":"USDT","status":"TRADING"},
          {"symbol":"XRPBTC","baseAsset":"XRP","quoteAsset":"BTC","status":"BREAK"}
        ]
      }
      """;

  private static final String FUTURES_SAMPLE =
      """
      {
        "symbols": [
          {"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT",
            "contractType":"PERPETUAL","status":"TRADING"},
          {"symbol":"ETHUSDT","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT",
            "contractType":"PERPETUAL","status":"TRADING"},
          {"symbol":"SOLUSDT_250328","baseAsset":"SOL","quoteAsset":"USDT",
            "contractType":"CURRENT_QUARTER","status":"TRADING"}
        ]
      }
      """;

  @Test
  void spot_filters_out_non_trading_symbols() {
    Set<Instrument> parsed =
        BinanceInstrumentsCache.parse(SPOT_SAMPLE, BinanceInstrumentsCache.Market.SPOT);
    assertThat(parsed)
        .containsExactlyInAnyOrder(
            new Instrument("BTC", "USDT"),
            new Instrument("ETH", "USDT"));
  }

  @Test
  void futures_keeps_only_perpetuals_and_encodes_settle_currency() {
    Set<Instrument> parsed =
        BinanceInstrumentsCache.parse(FUTURES_SAMPLE, BinanceInstrumentsCache.Market.FUTURES_USDT);
    assertThat(parsed)
        .containsExactlyInAnyOrder(
            new Instrument("BTC", "USDT", "USDT"),
            new Instrument("ETH", "USDT", "USDT"));
  }
}
