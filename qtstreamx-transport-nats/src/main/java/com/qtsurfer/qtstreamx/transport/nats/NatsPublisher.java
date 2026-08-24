package com.qtsurfer.qtstreamx.transport.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import com.qtsurfer.qtstreamx.core.transport.TransportPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NATS transport publisher. Publishes encoded bytes to NATS subjects.
 */
public class NatsPublisher implements TransportPublisher {

    private static final Logger log = LoggerFactory.getLogger(NatsPublisher.class);
    private final Connection connection;

    public NatsPublisher(String url) throws Exception {
        this.connection = Nats.connect(Options.builder().server(url).build());
        log.info("Connected to NATS at {}", url);
    }

    public NatsPublisher(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void publish(String subject, byte[] data) {
        connection.publish(subject, data);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
