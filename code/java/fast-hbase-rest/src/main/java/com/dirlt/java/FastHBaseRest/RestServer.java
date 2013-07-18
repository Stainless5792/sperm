package com.dirlt.java.FastHBaseRest;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 1:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class RestServer {
    public static class Logger {
        public boolean xdebug = true;
        public boolean xinfo = true;
        public boolean xwarn = true;

        public void debug(String s) {
            if (xdebug) {
                System.out.println("[DEBUG]" + s);
            }
        }

        public void info(String s) {
            if (xinfo) {
                System.out.println("[INFO]" + s);
            }
        }

        public void warn(String s) {
            if (xwarn) {
                System.out.println("[WARN]" + s);
            }
        }
    }

    public static Logger logger = new Logger();

    public static void runHttpServer(final Configuration configuration) {
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newSingleThreadExecutor(),
                new ThreadPoolExecutor(configuration.getIoThreadNumber(), configuration.getIoThreadNumber(),
                        0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(configuration.getIoQueueSize())),
                configuration.getIoThreadNumber());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpChunkAggregator(1024 * 1024 * 8));
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("handler", new RestHandler(configuration));
                return pipeline;
            }
        });
        bootstrap.bind(new InetSocketAddress(configuration.getIp(), configuration.getPort()));
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        if (!configuration.parse(args)) {
            Configuration.usage();
            return;
        }
        // easy way!!
        if (!configuration.isDebug()) {
            logger.xdebug = false;
        }
        System.out.print(configuration);
        LocalCache.init(configuration);
        CpuWorkerPool.init(configuration);
        HBaseService.init(configuration);
        StatStore.init(configuration);
        RequestProxy.init(configuration);
        runHttpServer(configuration);
    }
}
