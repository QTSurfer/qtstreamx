package com.qtsurfer.qtstreamx.evm.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qtsurfer.qtstreamx.core.ws.WebSocketClient;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/**
 * Between the end of the history read over HTTP and the first block the websocket delivers, nothing used to ask for the blocks
 * mined meanwhile: the history stops at {@code latest - confirmationDepth} as read when it began, and a {@code logs}
 * subscription only delivers what is mined after it is active. On a chain that mines several blocks a second and a history that
 * takes minutes, that is minutes of swaps that never reach the handler.
 *
 * <p>The chain here mines while it answers a history request, as a real one does, and its websocket delivers only the blocks
 * mined after the subscription was sent. Every swap has to reach the handler once, in chain order, on each of the three ways a
 * stream connects: {@code start}, a reconnection and {@code startRecoverable}.
 */
class EvmRpcLogStreamGapTest {

    private static final EvmLogFilter FILTER = new EvmLogFilter(Set.of("0xpool"), Set.of("0xswap"));
    private static final EvmLogStreamId STREAM_ID = new EvmLogStreamId("eip155:1", "gap-test");

    @Test
    void startDeliversEverySwapOnceInChainOrderWhenTheChainAdvancesWhileTheHistoryIsRead() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 3);
        ChainWebSocket webSocket = chain.newSocket();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config(5), () -> webSocket, chain)) {
            stream.start(FILTER, emitted::add);
            chain.mine(10);

            assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.LIVE);
        }

        assertThat(blocksOf(emitted)).containsExactlyElementsOf(range(100, chain.head - 2));
    }

    @Test
    void reconnectionDeliversEverySwapOnceInChainOrderWhenTheChainAdvancesWhileItCatchesUp() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 0);
        ChainWebSocket first = chain.newSocket();
        int[] opened = {0};
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(
                config(5),
                () -> opened[0]++ == 0 ? first : chain.newSocket(),
                chain,
                ReconnectScheduler.immediate())) {
            stream.start(FILTER, emitted::add);
            chain.mine(4);

            // the connection drops; reading what was missed takes a while, and the chain keeps mining meanwhile
            chain.minedPerRead = 3;
            first.disconnect();
            chain.mine(10);
        }

        assertThat(blocksOf(emitted)).doesNotHaveDuplicates();
        assertThat(blocksOf(emitted)).containsExactlyElementsOf(range(100, chain.head - 2));
    }

    @Test
    void startRecoverableDeliversEverySwapOnceInChainOrderWhenTheChainAdvancesWhileTheHistoryIsRead() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 3);
        ChainWebSocket webSocket = chain.newSocket();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmRpcLogStream stream = new EvmRpcLogStream(
                config(5),
                () -> webSocket,
                chain,
                ReconnectScheduler.immediate(),
                checkpoints,
                new EvmRecoveryPolicy(STREAM_ID, "provider-a", 2, 10_000))) {
            stream.startRecoverable(FILTER, batch -> {
                emitted.addAll(batch.logs());
                return EvmLogAcknowledgement.ACKNOWLEDGED;
            });
            chain.mine(10);
        }

        assertThat(blocksOf(emitted)).containsExactlyElementsOf(range(100, chain.head - 2));
    }

    @Test
    void aLogThatArrivesOverHttpAndOverTheWebSocketIsDeliveredOnce() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 0);
        ChainWebSocket webSocket = chain.newSocket();
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config(5), () -> webSocket, chain)) {
            stream.start(FILTER, emitted::add);
            // the websocket repeats a block the history already read, as a provider may after a reconnection
            webSocket.emit(chain.logNotification(109));
            chain.mine(5);
        }

        assertThat(blocksOf(emitted)).doesNotHaveDuplicates();
        assertThat(blocksOf(emitted)).containsExactlyElementsOf(range(100, chain.head - 2));
    }

    @Test
    void readsTheHeadOfTheLastPassOnlyAfterBothSubscriptionsAreAcknowledged() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 0);
        ChainWebSocket webSocket = chain.newSocket();
        webSocket.ackDelayMillis = 150; // the provider answers after a while, as a real one does
        List<EvmLog> emitted = new ArrayList<>();

        try (EvmLogStream stream = new EvmRpcLogStream(config(5), () -> webSocket, chain)
                .awaitSubscriptionAcks(Duration.ofSeconds(5))) {
            stream.start(FILTER, emitted::add);
            chain.mine(5);
        }

        assertThat(chain.acksAtHeadRead).as("acknowledgements seen when the head was read, once subscribed")
                .containsExactly(2);
        assertThat(blocksOf(emitted)).containsExactlyElementsOf(range(100, chain.head - 2));
    }

    @Test
    void failsTheConnectionWhenTheProviderNeverAcknowledgesTheSubscriptions() throws Exception {
        AdvancingChain chain = new AdvancingChain(100, 110, 0);
        ChainWebSocket webSocket = chain.newSocket();
        webSocket.ackDelayMillis = -1;
        List<Throwable> errors = new ArrayList<>();

        try (EvmRpcLogStream stream = new EvmRpcLogStream(config(5), () -> webSocket, chain)
                .awaitSubscriptionAcks(Duration.ofMillis(100))) {
            stream.onError(errors::add);
            assertThatThrownBy(() -> stream.start(FILTER, ignored -> {}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("acknowledge");
            assertThat(stream.status()).isEqualTo(EvmLogStreamStatus.FAILED);
        }
    }

    private static List<Long> blocksOf(List<EvmLog> logs) {
        return logs.stream().map(EvmLog::blockNumber).toList();
    }

    private static List<Long> range(long from, long to) {
        return LongStream.rangeClosed(from, to).boxed().toList();
    }

    private static EvmLogStreamConfig config(int maxBlockRange) {
        return new EvmLogStreamConfig(
                "eip155:1",
                "wss://rpc.invalid/ws",
                "https://rpc.invalid/http",
                100,
                2,
                maxBlockRange,
                Duration.ofSeconds(5),
                3);
    }

    /** One swap in every block from {@code firstSwapBlock}; mines {@code minedPerRead} blocks while it answers a history request. */
    private static final class AdvancingChain implements EvmRpcHttpClient {
        private final long firstSwapBlock;
        private long head;
        private int minedPerRead;
        private ChainWebSocket socket;
        private final List<Integer> acksAtHeadRead = new ArrayList<>();

        private AdvancingChain(long firstSwapBlock, long head, int minedPerRead) {
            this.firstSwapBlock = firstSwapBlock;
            this.head = head;
            this.minedPerRead = minedPerRead;
        }

        ChainWebSocket newSocket() {
            socket = new ChainWebSocket();
            return socket;
        }

        void mine(int blocks) {
            for (int i = 0; i < blocks; i++) {
                head++;
                if (socket != null && socket.subscribed) {
                    socket.emit(logNotification(head));
                    socket.emit(headNotification(head));
                }
            }
        }

        @Override
        public long latestBlockNumber() {
            if (socket != null && socket.subscribed) {
                acksAtHeadRead.add(socket.acks.get());
            }
            return head;
        }

        @Override
        public EvmBlock getBlock(long number) {
            return number <= head
                    ? new EvmBlock(number, "0xblock" + number, 1_700_000_000_000_000L + number)
                    : null;
        }

        @Override
        public List<EvmRpcLog> getLogs(EvmLogFilter filter, long fromBlock, long toBlock) {
            List<EvmRpcLog> answer = new ArrayList<>();
            for (long block = Math.max(fromBlock, firstSwapBlock); block <= Math.min(toBlock, head); block++) {
                answer.add(new EvmRpcLog(
                        "0xpool", List.of("0xswap"), "0xdata", block, "0xblock" + block,
                        "0xtx" + block, 0, 1, false));
            }
            mine(minedPerRead); // time passes while the provider answers
            return answer;
        }

        String logNotification(long block) {
            return """
                    {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xlogs","result":{
                      "address":"0xpool","topics":["0xswap"],"data":"0xdata","removed":false,
                      "blockNumber":"0x%x","blockHash":"0xblock%d","transactionHash":"0xtx%d",
                      "transactionIndex":"0x0","logIndex":"0x1"}}}
                    """.formatted(block, block, block);
        }

        String headNotification(long block) {
            return """
                    {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xheads","result":{
                      "number":"0x%x","hash":"0xblock%d"}}}
                    """.formatted(block, block);
        }
    }

    /** Delivers only what is mined after the subscription request was sent. */
    private static final class ChainWebSocket implements WebSocketClient {
        private static final Pattern REQUEST_ID = Pattern.compile("\"id\":(\\d+)");

        private volatile boolean open;
        private volatile boolean subscribed;
        private volatile int ackDelayMillis;
        private final AtomicInteger acks = new AtomicInteger();
        private Consumer<String> messageHandler;
        private BiConsumer<Integer, String> closeHandler;

        @Override
        public void connect(String url) {
            open = true;
        }

        @Override
        public void send(String message) {
            if (message.contains("\"logs\"")) {
                subscribed = true;
            }
            Matcher id = REQUEST_ID.matcher(message);
            if (ackDelayMillis < 0 || !id.find()) {
                return;
            }
            String ack = "{\"jsonrpc\":\"2.0\",\"id\":" + id.group(1) + ",\"result\":\"0xsub" + id.group(1) + "\"}";
            if (ackDelayMillis == 0) {
                acks.incrementAndGet();
                messageHandler.accept(ack);
                return;
            }
            Thread answer = new Thread(() -> {
                try {
                    Thread.sleep(ackDelayMillis);
                } catch (InterruptedException interrupted) {
                    return;
                }
                acks.incrementAndGet();
                messageHandler.accept(ack);
            });
            answer.setDaemon(true);
            answer.start();
        }

        @Override
        public void onMessage(Consumer<String> handler) {
            messageHandler = handler;
        }

        @Override
        public void onClose(BiConsumer<Integer, String> handler) {
            closeHandler = handler;
        }

        @Override
        public void onError(Consumer<Throwable> handler) {}

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
            subscribed = false;
        }

        void emit(String message) {
            if (open && messageHandler != null) {
                messageHandler.accept(message);
            }
        }

        void disconnect() {
            open = false;
            subscribed = false;
            if (closeHandler != null) {
                closeHandler.accept(1006, "test disconnect");
            }
        }
    }

    private static final class InMemoryCheckpointStore implements EvmLogCheckpointStore {
        private final Map<EvmLogStreamId, EvmLogCheckpoint> checkpoints = new HashMap<>();

        @Override
        public Optional<EvmLogCheckpoint> load(EvmLogStreamId streamId) {
            return Optional.ofNullable(checkpoints.get(streamId));
        }

        @Override
        public void save(EvmLogCheckpoint checkpoint) {
            checkpoints.put(checkpoint.streamId(), checkpoint);
        }
    }
}
