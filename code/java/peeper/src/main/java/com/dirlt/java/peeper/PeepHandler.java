package com.dirlt.java.peeper;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
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

public class PeepHandler extends SimpleChannelHandler {
    private static final Set<String> allowedPath = new TreeSet<String>();

    static {
        allowedPath.add("/stat");
        allowedPath.add("/read");
    }

    private Configuration configuration;
    private AsyncClient client; // binding to the channel pipeline.

    public PeepHandler(Configuration configuration) {
        this.configuration = configuration;
        client = new AsyncClient(configuration); // each handler corresponding a channel or a connection.
    }

    private void writeContent(Channel channel, String content) {
        // so simple.
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.setHeader("Content-Length", content.length());
        ChannelBuffer buffer = ChannelBuffers.buffer(content.length());
        buffer.writeBytes(content.getBytes());
        response.setContent(buffer);
        channel.write(response);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        PeepServer.logger.debug("peeper message received");
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
            stat.addCounter("peeper.uri.invalid.count", 1);
            channel.close();
            return;
        }

        // invalid path.
        if (!allowedPath.contains(path)) {
            stat.addCounter("peeper.uri.unknown.count", 1);
            channel.close(); // just close the connection.
            return;
        }

        // as stat, we can easily handle it.
        if (path.equals("/stat")) {
            // TODO(dirlt):add more info.
            String content = StatStore.getStat();
            writeContent(channel, content);
            return;
        }

        stat.addCounter("peeper.rpc.in.count", 1);
        client.externalChannel = channel;
        client.path = path;
        client.buffer = request.getContent();
        client.requestTimestamp = System.currentTimeMillis();
        client.run();
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        PeepServer.logger.debug("peeper write completed");

        if (client.code != AsyncClient.Status.kStat) {
            StatStore.getInstance().addCounter("peeper.rpc.out.count", 1);
        }
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        PeepServer.logger.debug("peeper.connection open");
        StatStore.getInstance().addCounter("peeper.session.in.count", 1);
        client.sessionStartTimestamp = System.currentTimeMillis();
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        PeepServer.logger.debug("peeper connection closed");

        StatStore.getInstance().addCounter("peeper.session.out.count", 1);
        client.sessionEndTimestamp = System.currentTimeMillis();
        StatStore.getInstance().addCounter("peeper.session.duration", client.sessionEndTimestamp - client.sessionStartTimestamp);
        e.getChannel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        // e.getCause() instanceof ReadTimeoutException
        // e.getCause() instanceof WriteTimeoutException

        PeepServer.logger.debug("peeper exception caught");
        StatStore.getInstance().addCounter("peeper.exception.count", 1);
        e.getChannel().close();
    }
}