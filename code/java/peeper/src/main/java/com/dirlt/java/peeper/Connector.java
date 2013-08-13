package com.dirlt.java.peeper;

import org.jboss.netty.bootstrap.Bootstrap;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

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
    private BlockingQueue<AsyncClient> requestQueue = null;
    private Bootstrap bootstrap = null;

    class Node {
        public InetSocketAddress socketAddress;
        public float weight;
        // more fields.
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
        // TODO(dirlt): maybe add more connection.
    }

    public void onChannelClosed(Channel channel) {
        // channel will be closed.
        // TODO(dirlt): maybe add more connection.
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
        parseNodes();
    }

    public void parseNodes() {
        nodes = new HashMap<String, Node>();
        String backendNodes = configuration.getBackendNodes();
        for (String s : backendNodes.split(",")) {
            String[] ss = s.split(":");
            String host = ss[0];
            int port = Integer.valueOf(ss[1]).intValue();
            Node node = new Node();
            node.socketAddress = new InetSocketAddress(host, port);
            // TODO(dirlt): more adaptive.
            node.weight = 1.0f;
            nodes.put(node.socketAddress.toString(), node);
        }
    }
}
