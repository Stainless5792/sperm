package com.dirlt.java.peeper;

import org.jboss.netty.channel.*;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 8/13/13
 * Time: 2:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProxyHandler extends SimpleChannelHandler {
    private Configuration configuration;
    private Connector connector;
    private boolean connected = false;

    public ProxyHandler(Configuration configuration, Connector connector) {
        this.configuration = configuration;
        this.connector = connector;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        connected = true;
        // TODO(dirlt):
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        // TODO(dirlt):
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        // TODO(dirlt):
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        // e.getCause() instanceof ReadTimeoutException
        // e.getCause() instanceof WriteTimeoutException
        connector.onChannelClosed(e.getChannel(),connected);
        e.getChannel().close();
    }
}
