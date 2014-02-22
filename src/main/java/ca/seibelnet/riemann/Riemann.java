package ca.seibelnet.riemann;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.RiemannBatchClient;

import java.io.Closeable;
import java.io.IOException;

public class Riemann implements Closeable {

    String riemannHost;
    Integer riemannPort;

    AbstractRiemannClient client;

    public Riemann(String host, Integer port) throws IOException {
        this(host, port, 10);
    }
    public Riemann(String host, Integer port, int batchSize) throws IOException {
        this.riemannHost = host;
        this.riemannPort = port;
        this.client = new RiemannBatchClient(batchSize,
                RiemannClient.tcp(riemannHost, riemannPort));
    }

    public void connect() throws IOException {
        if (!client.isConnected()) {
            client.connect();
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.disconnect();
        }

    }

}
