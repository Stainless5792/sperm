package com.dirlt.java.FastHBaseRest;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.handler.codec.http.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 1:44 AM
 * To change this template use File | Settings | File Templates.
 */

public class RestHandler extends SimpleChannelHandler {
    private static final Set<String> allowedPath = new TreeSet<String>();

    static {
        allowedPath.add("/stat");
        allowedPath.add("/read");
        allowedPath.add("/multi-read");
        allowedPath.add("/write");
        allowedPath.add("/multi-write");
        allowedPath.add("/clear-cache");
    }

    private Configuration configuration;
    private AsyncClient client; // binding to the channel pipeline.

    public RestHandler(Configuration configuration) {
        this.configuration = configuration;
        client = new AsyncClient(configuration); // each handler corresponding a channel or a connection.
    }

    private void writeContent(Channel channel, String content) {
        HttpResponse response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.setHeader("Content-Length", content.length());
        ChannelBuffer buffer = ChannelBuffers.buffer(content.length());
        buffer.writeBytes(content.getBytes());
        response.setContent(buffer);
        channel.write(response);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        RestServer.logger.debug("message received");
        HttpRequest request = (HttpRequest) e.getMessage();
        Channel channel = e.getChannel();
        StatStore stat = StatStore.getInstance();
        String path = null;

        // invalid uri.
        try {
            URI uri = new URI(request.getUri());
            path = uri.getPath();
            if (path.isEmpty() || path.equals("/")) {
                path = "/stat";
            }
        } catch (URISyntaxException ex) {
            // ignore.
            stat.addCounter("uri.invalid.count", 1);
            channel.close();
            return;
        }

        // invalid path.
        if (!allowedPath.contains(path)) {
            stat.addCounter("uri.unknown.count", 1);
            channel.close(); // just close the connection.
            return;
        }

        // as stat, we can easily handle it.
        if (path.equals("/stat")) {
            String content = StatStore.getStat();
            writeContent(channel, content);
            return;
        }

        if (path.equals("/clear-cache")) {
            String content = "OK";
            LocalCache.getInstance().clear();
            writeContent(channel, content);
            return;
        }

        stat.addCounter("rpc.in.count", 1);
        client.init();
        client.code = AsyncClient.Status.kHttpRequest;
        client.subRequest = false;
        client.channel = channel;
        client.path = path;
        client.buffer = request.getContent();
        client.requestTimestamp = System.currentTimeMillis();
        client.run();
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx,
                              WriteCompletionEvent e) {
        RestServer.logger.debug("write completed");

        if (client.code != AsyncClient.Status.kStat) {
            StatStore.getInstance().addCounter("rpc.out.count", 1);
        }
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        RestServer.logger.debug("connection open");

        StatStore.getInstance().addCounter("session.in.count", 1);
        client.sessionStartTimestamp = System.currentTimeMillis();

        Channel channel = ctx.getChannel();
        SocketChannelConfig config = (SocketChannelConfig) channel.getConfig();
        config.setKeepAlive(true);
        config.setTcpNoDelay(true);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        RestServer.logger.debug("connection closed");

        StatStore.getInstance().addCounter("session.out.count", 1);
        client.sessionEndTimestamp = System.currentTimeMillis();
        StatStore.getInstance().addCounter("session.duration",
                client.sessionEndTimestamp - client.sessionStartTimestamp);
        e.getChannel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        RestServer.logger.debug("exception caught");

        StatStore.getInstance().addCounter("exception.count", 1);
        // seems there is no particular request takes a lot time.
//        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}