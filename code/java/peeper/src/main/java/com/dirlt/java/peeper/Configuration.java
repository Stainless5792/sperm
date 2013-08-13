package com.dirlt.java.peeper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 1:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class Configuration {
    private String ip = "0.0.0.0";
    private int port = 8001;
    private int backlog = 128;
    private String backendNodes = "localhost:12345";
    private int acceptIOThreadNumber = 4;
    private int ioThreadNumber = 16;
    private int readTimeout = 10; // 10s.
    private int writeTimeout = 10; // 10s.
    private int proxyQueueSize = 4096;
    private int proxyAcceptIOThreadNumber = 4;
    private int proxyIOThreadNumber = 16;
    private int proxyReadTimeout = 10;
    private int proxyWriteTimeout = 10;
    private String serviceName = "peeper";
    private boolean debug = true;
    private boolean stat = true;
    private Map<String, String> kv = new HashMap<String, String>();

    public boolean parse(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--ip=")) {
                ip = arg.substring("--ip=".length());
            } else if (arg.startsWith("--port=")) {
                port = Integer.valueOf(arg.substring("--port=".length())).intValue();
            } else if (arg.startsWith("--backlog=")) {
                backlog = Integer.valueOf(arg.substring("--backlog=".length())).intValue();
            } else if (arg.startsWith("--backend-nodes=")) {
                backendNodes = arg.substring("--backend-nodes=".length());
            } else if (arg.startsWith("--accept-io-thread-number=")) {
                acceptIOThreadNumber = Integer.valueOf(arg.substring("--accept-io-thread-number=".length()));
            } else if (arg.startsWith("--io-thread-number=")) {
                ioThreadNumber = Integer.valueOf(arg.substring("--io-thread-number=".length())).intValue();
            } else if (arg.startsWith("--read-timeout=")) {
                readTimeout = Integer.valueOf(arg.substring("--read-timeout=".length())).intValue();
            } else if (arg.startsWith("--write-timeout=")) {
                writeTimeout = Integer.valueOf(arg.substring("--write-timeout=".length())).intValue();
            } else if (arg.startsWith("--proxy-queue-size=")) {
                proxyQueueSize = Integer.valueOf(arg.substring("--proxy-queue-size=".length())).intValue();
            } else if (arg.startsWith("--proxy-accept-io-thread-number=")) {
                proxyAcceptIOThreadNumber = Integer.valueOf(arg.substring("--proxy-accept-io-thread-number=".length())).intValue();
            } else if (arg.startsWith("--proxy-io-thread-number=")) {
                proxyIOThreadNumber = Integer.valueOf(arg.substring("--proxy-io-thread-number=".length())).intValue();
            } else if (arg.startsWith("--proxy-read-timeout=")) {
                proxyReadTimeout = Integer.valueOf(arg.substring("--proxy-read-timeout=".length())).intValue();
            } else if (arg.startsWith("--proxy-write-timeout=")) {
                proxyWriteTimeout = Integer.valueOf(arg.substring("--proxy-write-timeout=".length())).intValue();
            } else if (arg.startsWith("--service-name=")) {
                serviceName = arg.substring("--service-name=".length());
            } else if (arg.startsWith("--no-debug")) {
                debug = false;
            } else if (arg.startsWith("--no-stat")) {
                stat = false;
            } else if (arg.startsWith("--kv=")) {
                String s = arg.substring("--kv=".length());
                String[] ss = s.split(":");
                kv.put(ss[0], ss[1]);
            } else {
                return false;
            }
        }
        return true;
    }

    public static void usage() {
        System.out.println("peeper");
        System.out.println("\t--ip # default 0.0.0.0");
        System.out.println("\t--port # default 8001");
        System.out.println("\t--backlog # default 128");
        System.out.println("\t--backend-nodes # default localhost:12345");
        System.out.println("\t--accept-io-thread-number # default 4");
        System.out.println("\t--io-thread-number # default 16");
        System.out.println("\t--read-timeout # default 10(s)");
        System.out.println("\t--write-timeout # default 10(s)");
        System.out.println("\t--proxy-queue-size # default 4096");
        System.out.println("\t--proxy-accept-io-thread-number # default 4");
        System.out.println("\t--proxy-io-thread-number # default 16");
        System.out.println("\t--proxy-read-timeout # default 10(s)");
        System.out.println("\t--proxy-write-timeout # default 10(s)");
        System.out.println("\t--service-name # set service name");
        System.out.println("\t--no-debug # turn off debug mode");
        System.out.println("\t--no-stat # turn off statistics");
        System.out.println("\t--kv=<key>:<value> # key value pair");
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("stat=%s, debug=%s\n", isStat(), isDebug()));
        sb.append(String.format("ip=%s, port=%d, backlog=%d\n", getIp(), getPort(), getBacklog()));
        sb.append(String.format("backend-nodes=%s\n", getBackendNodes()));
        sb.append(String.format("service-name=%s\n", getServiceName()));
        sb.append(String.format("accept-io-thread-number=%d, io-thread-number=%d\n", getAcceptIOThreadNumber(), getIoThreadNumber()));
        sb.append(String.format("read-timeout=%d(s), write-timeout=%d(s)", getReadTimeout(), getWriteTimeout()));
        sb.append(String.format("proxy-queue-size=%d\n", getProxyQueueSize()));
        sb.append(String.format("proxy-accept-io-thread-number=%d, proxy-io-thread-number=%d\n",
                getProxyAcceptIOThreadNumber(), getProxyIOThreadNumber()));
        sb.append(String.format("proxy-read-timeout=%d(s), proxy-write-timeout=%d(s)", getProxyReadTimeout(), getProxyWriteTimeout()));
        for (String key : kv.keySet()) {
            sb.append(String.format("kv = %s:%s\n", key, kv.get(key)));
        }
        return sb.toString();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getBacklog() {
        return backlog;
    }

    public String getBackendNodes() {
        return backendNodes;
    }

    public int getAcceptIOThreadNumber() {
        return acceptIOThreadNumber;
    }

    public int getIoThreadNumber() {
        return ioThreadNumber;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public int getProxyQueueSize() {
        return proxyQueueSize;
    }

    public int getProxyAcceptIOThreadNumber() {
        return proxyAcceptIOThreadNumber;
    }

    public int getProxyIOThreadNumber() {
        return proxyIOThreadNumber;
    }

    public int getProxyReadTimeout() {
        return proxyReadTimeout;
    }

    public int getProxyWriteTimeout() {
        return proxyWriteTimeout;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isDebug() {
        return debug;
    }

    public boolean isStat() {
        return stat;
    }

    public Map<String, String> getKv() {
        return kv;
    }
}
