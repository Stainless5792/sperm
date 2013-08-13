package com.dirlt.java.peeper;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.handler.timeout.WriteTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 8/13/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class Connector {
    private static Connector instance = null;

    private Configuration configuration;
    private AtomicInteger connectionNumber = new AtomicInteger(0);
    private BlockingQueue<AsyncClient> requestQueue = null;
    private ClientBootstrap bootstrap = null;
    public static final int kTickInterval = 1 * 1000; // 1s;
    // each time addConnection will create at least following number connections.
    public static final int kAddConnectionStep = 4;

    class Node {
        public InetSocketAddress socketAddress;
        public float staticWeight;
        // more fields.
        public static final int kMaxFailureCount = 10;
        public static final int kConnectRecoveryTickNumber = 10; // 10s.
        public static final int kReadWriteRecoveryTickNumber = 2; // 2s.
        int connectFailureCount = 0;
        int readWriteFailureCount = 0;
    }

    private Map<String, Node> nodes = null;

    public static void init(Configuration configuration) {
        instance = new Connector(configuration);
    }

    public static Connector getInstance() {
        return instance;
    }

    public void request(AsyncClient client) {
        requestQueue.add(client);
    }

    public void onChannelClosed(Channel channel, boolean connected) {
        // have not been already connected.
        Node node = nodes.get(channel.getRemoteAddress().toString());
        if (node == null) {
            PeepServer.logger.warn("unknown remote address " + channel.getRemoteAddress().toString());
        }
        if (!connected) {
            if (node != null) {
                // so here we add one failure.
                synchronized (node) {
                    if (node.connectFailureCount < Node.kMaxFailureCount) {
                        node.connectFailureCount += 1;
                    }
                }
            }
            connectionNumber.decrementAndGet();
        } else {
            if (node != null) {
                synchronized (node) {
                    if (node.readWriteFailureCount < Node.kMaxFailureCount) {
                        node.readWriteFailureCount += 1;
                    }
                }
            }
            // just do reconnect.
            bootstrap.connect(channel.getRemoteAddress());
        }
    }

    public void addConnection() {
        // TODO(dirlt):
    }

    public Connector(final Configuration configuration) {
        this.configuration = configuration;
        requestQueue = new LinkedBlockingQueue<AsyncClient>(configuration.getProxyQueueSize());
        final Timer timer = new HashedWheelTimer();
        ChannelFactory channelFactory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                configuration.getProxyAcceptIOThreadNumber(),
                configuration.getProxyIOThreadNumber());
        bootstrap = new ClientBootstrap(channelFactory);
        final Connector connector = this;
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpChunkAggregator(1024 * 1024 * 8));
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("rto_handler", new ReadTimeoutHandler(timer, configuration.getProxyReadTimeout()));
                pipeline.addLast("wto_handler", new WriteTimeoutHandler(timer, configuration.getProxyWriteTimeout()));
                pipeline.addLast("handler", new ProxyHandler(configuration, connector));
                return pipeline;
            }
        });
        // parse nodes.
        nodes = new HashMap<String, Node>();
        String backendNodes = configuration.getBackendNodes();
        for (String s : backendNodes.split(",")) {
            String[] ss = s.split(":");
            String host = ss[0];
            int port = Integer.valueOf(ss[1]).intValue();
            Node node = new Node();
            node.socketAddress = new InetSocketAddress(host, port);
            node.staticWeight = 1.0f; // TODO(dirlt): some preference?
            nodes.put(node.socketAddress.toString(), node);
        }
        // make min connection.
        addConnection();
        // timer to decrease failure count.
        java.util.Timer tickTimer = new java.util.Timer(true);
        tickTimer.scheduleAtFixedRate(new TimerTask() {
            private int connectRecoveryTickCount = Node.kConnectRecoveryTickNumber;
            private int readWriteRecoveryTickCount = Node.kReadWriteRecoveryTickNumber;

            @Override
            public void run() {
                // connect.
                connectRecoveryTickCount -= 1;
                if (connectRecoveryTickCount == 0) {
                    for (Map.Entry<String, Node> entry : nodes.entrySet()) {
                        Node node = entry.getValue();
                        synchronized (node) {
                            if (node.connectFailureCount > 0) {
                                node.connectFailureCount -= 1;
                            }
                        }
                    }
                    connectRecoveryTickCount = Node.kConnectRecoveryTickNumber;
                }

                // read & write
                readWriteRecoveryTickCount -= 1;
                if (readWriteRecoveryTickCount == 0) {
                    for (Map.Entry<String, Node> entry : nodes.entrySet()) {
                        Node node = entry.getValue();
                        synchronized (node) {
                            if (node.readWriteFailureCount > 0) {
                                node.readWriteFailureCount -= 1;
                            }
                        }
                    }
                    readWriteRecoveryTickCount = Node.kReadWriteRecoveryTickNumber;
                }

                // connection.
                addConnection();
            }
        }, 0, kTickInterval);
    }
}
