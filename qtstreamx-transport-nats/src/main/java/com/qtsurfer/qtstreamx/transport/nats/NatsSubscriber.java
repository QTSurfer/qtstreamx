package com.qtsurfer.qtstreamx.transport.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import com.qtsurfer.qtstreamx.core.transport.TransportSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * NATS transport subscriber. Delivers raw bytes from NATS subjects to handlers.
 */
public class NatsSubscriber implements TransportSubscriber {

    private static final Logger log = LoggerFactory.getLogger(NatsSubscriber.class);
    private final Connection connection;
    private final Dispatcher dispatcher;

    public NatsSubscriber(String url) throws Exception {
        this.connection = Nats.connect(Options.builder().server(url).build());
        this.dispatcher = connection.createDispatcher();
        log.info("Connected to NATS at {}", url);
    }

    public NatsSubscriber(Connection connection) {
        this.connection = connection;
        this.dispatcher = connection.createDispatcher();
    }

    @Override
    public void subscribe(String subject, Consumer<byte[]> handler) {
        dispatcher.subscribe(subject, msg -> handler.accept(msg.getData()));
        log.debug("Subscribed to {}", subject);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
