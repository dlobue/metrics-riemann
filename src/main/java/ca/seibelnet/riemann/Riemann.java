package ca.seibelnet.riemann;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.RiemannBatchClient;
import com.aphyr.riemann.client.UnsupportedJVMException;


import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;


public class Riemann implements Closeable {

    String riemannHost;
    Integer riemannPort;

    AbstractRiemannClient client;

    public Riemann(String host, Integer port) throws IOException, UnknownHostException {
        this(host, port, 10);
    }
    public Riemann(String host, Integer port, int batchSize) throws IOException, UnknownHostException {
        this.riemannHost = host;
        this.riemannPort = port;
        RiemannClient c = RiemannClient.tcp(riemannHost, riemannPort);
        try {
            this.client = new RiemannBatchClient(batchSize,c);
        } catch (UnsupportedJVMException e) {
            this.client = c;
        }
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
