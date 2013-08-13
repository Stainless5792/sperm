package com.dirlt.java.peeper;


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 8/13/13
 * Time: 1:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class AsyncClient {
    private Configuration configuration;

    enum Status {
        kStat,
    }

    public Status code;
    public Channel externalChannel;
    public String path;
    public ChannelBuffer buffer;
    public long requestTimestamp;
    public long sessionStartTimestamp;
    public long sessionEndTimestamp;

    public AsyncClient(Configuration configuration) {
        this.configuration = configuration;
    }

    public void init() {

    }

    public void run() {

    }
}
