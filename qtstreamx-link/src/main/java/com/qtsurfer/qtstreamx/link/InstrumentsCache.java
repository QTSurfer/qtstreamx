package com.qtsurfer.qtstreamx.link;

import com.qtsurfer.qtstreamx.core.model.Instrument;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Periodically refreshed view of the instruments tradable on one exchange.
 *
 * <p>Exchange-specific implementations hit a REST endpoint (e.g. Binance
 * {@code /api/v3/exchangeInfo}) to retrieve the current instrument set.
 * {@link LinkManager} polls {@link #refresh()} on a schedule and diffs the
 * result against its current view to schedule live SUBSCRIBE/UNSUBSCRIBE calls.
 *
 * <p>Implementations are expected to be thread-safe: {@link #snapshot()} may be
 * called concurrently with an in-flight {@link #refresh()}.
 */
public interface InstrumentsCache {

  /** Exchange + market identifier, e.g. "binance-spot" or "binance-futures". */
  String exchangeKey();

  /**
   * Fetch the latest instrument list from the exchange. The returned future completes
   * with the up-to-date set; the cached {@link #snapshot()} is also updated.
   */
  CompletionStage<Set<Instrument>> refresh();

  /**
   * Instruments known as of the last successful {@link #refresh()}. Returns an empty
   * set before the first refresh has landed. Never returns {@code null}.
   */
  Set<Instrument> snapshot();

  /** True once at least one refresh has succeeded. */
  boolean isLoaded();
}
