package com.dirlt.java.FastHBaseRest;

import com.dirlt.java.FastHbaseRest.MessageProtos1;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 2:03 AM
 * To change this template use File | Settings | File Templates.
 */

public class AsyncClient implements Runnable {
    public static final String kSep = String.format("%c", 0x0);
    public static final long kDefaultTimeout = 100 * 1000; // 100s.(that's long enough).
    public static final byte[] kEmptyBytes = new byte[0];

    public Configuration configuration;

    public AsyncClient(Configuration configuration) {
        this.configuration = configuration;
    }

    // state of each step.
    enum Status {
        kStat,
        kMultiRead,
        kMultiWrite,

        kHttpRequest,
        kReadRequest,
        kWriteRequest,
        kReadLocalCache,
        kReadHBaseService,
        kWriteHBaseService,
        kReadResponse,
        kWriteResponse,
        kHttpResponse,
    }

    // sub request status
    enum SubRequestStatus {
        kOK,
        kException,
        kTimeout,
    }

    public boolean subRequest = false; // whether is sub request.
    public Status code = Status.kStat; // default value.
    public SubRequestStatus subRequestStatus = SubRequestStatus.kOK;
    public Channel channel;

    // for multi interface.
    public AsyncClient parent;
    public AtomicInteger refCounter;
    public List<AsyncClient> clients;

    public ChannelBuffer buffer;
    public String path;
    public byte[] bs;
    public MessageProtos1.ReadRequest.Builder rdReqBuilder;
    public MessageProtos1.ReadRequest rdReq;
    public MessageProtos1.WriteRequest.Builder wrReqBuilder;
    public MessageProtos1.WriteRequest wrReq;
    public MessageProtos1.MultiReadRequest.Builder mRdReqBuilder;
    public MessageProtos1.MultiReadRequest multiReadRequest;
    public MessageProtos1.MultiWriteRequest.Builder mWrReqBuilder;
    public MessageProtos1.MultiWriteRequest multiWriteRequest;
    public MessageProtos1.ReadResponse.Builder rdRes;
    public MessageProtos1.WriteResponse.Builder wrRes;
    public MessageProtos1.MultiReadResponse.Builder mRdRes;
    public MessageProtos1.MultiWriteResponse.Builder mWrRes;
    public Message msg;

    public long requestTimestamp;
    public long requestTimeout;
    public long sessionStartTimestamp;
    public long sessionEndTimestamp;
    public long readStartTimestamp;
    public long readEndTimestamp;
    public long readHBaseServiceStartTimestamp;
    public long readHBaseServiceEndTimestamp;
    public long writeHBaseServiceStartTimestamp;
    public long writeHBaseServiceEndTimestamp;

    public String tableName;
    public String rowKey;
    public String columnFamily;

    public String prefix; // cache key prefix.

    public static String makeCacheKeyPrefix(String tableName, String rowKey, String cf) {
        return tableName + kSep + rowKey + kSep + cf;
    }

    public static String makeCacheKey(String prefix, String column) {
        return prefix + kSep + column;
    }

    private List<String> readCacheQualifiers; // qualifiers that to be queried from cache.
    private List<String> readHBaseQualifiers; // qualifiers that to be queried from hbase.
    // if == null, then read column family.

    public void init() {
        rdReq = null;
        wrReq = null;
        multiReadRequest = null;
        multiWriteRequest = null;
        subRequestStatus = SubRequestStatus.kOK;
        requestTimeout = kDefaultTimeout;
    }


    @Override
    public void run() {
        switch (code) {
            case kHttpRequest:
                handleHttpRequest();
                break;
            case kMultiRead:
                multiRead();
                break;
            case kMultiWrite:
                multiWrite();
                break;
            case kReadRequest:
                readRequest();
                break;
            case kWriteRequest:
                writeRequest();
                break;
            case kReadLocalCache:
                readLocalCache();
                break;
            case kReadHBaseService:
                readHBaseService();
                break;
            case kWriteHBaseService:
                writeHBaseService();
                break;
            case kReadResponse:
                readResponse();
                break;
            case kWriteResponse:
                writeResponse();
                break;
            case kHttpResponse:
                handleHttpResponse();
                break;
            default:
                break;
        }
    }

    public void handleHttpRequest() {
        RestServer.logger.debug("http request");
        int size = buffer.readableBytes();
        StatStore.getInstance().addCounter("rpc.in.bytes", size);
        bs = new byte[size];
        buffer.readBytes(bs);

        if (path.equals("/read")) {
            code = Status.kReadRequest;
        } else if (path.equals("/multi-read")) {
            code = Status.kMultiRead;
        } else if (path.equals("/write")) {
            code = Status.kWriteRequest;
        } else if (path.equals("/multi-write")) {
            code = Status.kMultiWrite;
        } else {
            // impossible.
        }

        // entry. if we want async mode, we put into cpu thread
        // otherwise we just run.
        if (configuration.isAsync()) {
            CpuWorkerPool.getInstance().submit(this);
        } else {
            run();
        }
    }

    public void readRequest() {
        RestServer.logger.debug("read request");
        if (!subRequest) {
            // parse request.
            rdReqBuilder = MessageProtos1.ReadRequest.newBuilder();
            try {
                rdReqBuilder.mergeFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                // just close channel.
                RestServer.logger.debug("parse message exception");
                StatStore.getInstance().addCounter("rpc.in.count.invalid", 1);
                channel.close();
            }
            rdReq = rdReqBuilder.build();
            StatStore.getInstance().addCounter("rpc.read.count", 1);
            if (rdReq.hasTimeout()) {
                requestTimeout = rdReq.getTimeout();
            }
        }
        // proxy.
        try {
            rdReq = RequestProxy.getInstance().handleReadRequest(rdReq);
        } catch (Exception e) {
            // just close channle.
            RestServer.logger.debug("request proxy exception");
            StatStore.getInstance().addCounter("request.proxy.exception", 1);
            channel.close();
        }

        readStartTimestamp = System.currentTimeMillis();

        tableName = rdReq.getTableName();
        rowKey = rdReq.getRowKey();
        columnFamily = rdReq.getColumnFamily();

        rdRes = MessageProtos1.ReadResponse.newBuilder();

        prefix = makeCacheKeyPrefix(tableName, rowKey, columnFamily);

        if (rdReq.getQualifiersCount() == 0) {
            // read column family
            // then we can't do cache.
            code = Status.kReadHBaseService;
        } else {
            // raise local cache request.
            code = Status.kReadLocalCache;
        }
        run();
    }

    public void multiRead() {
        RestServer.logger.debug("multi read request");
        mRdReqBuilder = MessageProtos1.MultiReadRequest.newBuilder();
        try {
            mRdReqBuilder.mergeFrom(bs);
        } catch (InvalidProtocolBufferException e) {
            // just close channel.
            RestServer.logger.debug("parse message exception");
            StatStore.getInstance().addCounter("rpc.in.count.invalid", 1);
            channel.close();
        }
        multiReadRequest = mRdReqBuilder.build();
        if (multiReadRequest.getRequestsCount() == 0) {
            RestServer.logger.debug("multi read no sub request");
            StatStore.getInstance().addCounter("rpc.multi-read.error.count", 1);
            channel.close();
            return;
        }
        StatStore.getInstance().addCounter("rpc.multi-read.count", 1);
        if (refCounter == null) {
            refCounter = new AtomicInteger(multiReadRequest.getRequestsCount());
        } else {
            refCounter.set(multiReadRequest.getRequestsCount());
        }
        if (clients == null) {
            clients = new LinkedList<AsyncClient>();
        } else {
            clients.clear();
        }
        if (multiReadRequest.hasTimeout()) {
            requestTimeout = multiReadRequest.getTimeout();
        }
        for (MessageProtos1.ReadRequest request : multiReadRequest.getRequestsList()) {
            AsyncClient client = new AsyncClient(configuration);
            client.init();
            client.code = Status.kReadRequest;
            client.subRequest = true;
            client.rdReq = request;
            client.parent = this;
            // sub request inherits from parent request.
            client.requestTimeout = requestTimeout;
            client.requestTimestamp = requestTimestamp;
            clients.add(client);
            CpuWorkerPool.getInstance().submit(client);
        }
    }

    public void writeRequest() {
        RestServer.logger.debug("write request");
        if (!subRequest) {
            wrReqBuilder = MessageProtos1.WriteRequest.newBuilder();
            // parse request.
            try {
                wrReqBuilder.mergeFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                // just close channel.
                RestServer.logger.debug("parse message exception");
                StatStore.getInstance().addCounter("rpc.in.count.invalid", 1);
                channel.close();
            }
            wrReq = wrReqBuilder.build();
            StatStore.getInstance().addCounter("rpc.write.count", 1);
            if (wrReq.hasTimeout()) {
                requestTimeout = wrReq.getTimeout();
            }
        }
        // proxy.
        try {
            wrReq = RequestProxy.getInstance().handleWriteRequest(wrReq);
        } catch (Exception e) {
            // just close channel.
            RestServer.logger.debug("request proxy exception");
            StatStore.getInstance().addCounter("request.proxy.exception", 1);
            channel.close();
        }

        tableName = wrReq.getTableName();
        rowKey = wrReq.getRowKey();
        columnFamily = wrReq.getColumnFamily();

        // prepare the result.
        wrRes = MessageProtos1.WriteResponse.newBuilder();

        code = Status.kWriteHBaseService;
        run();
    }

    public void multiWrite() {
        RestServer.logger.debug("multi write request");
        mWrReqBuilder = MessageProtos1.MultiWriteRequest.newBuilder();
        try {
            mWrReqBuilder.mergeFrom(bs);
        } catch (InvalidProtocolBufferException e) {
            // just close channel.
            RestServer.logger.debug("parse message exception");
            StatStore.getInstance().addCounter("rpc.in.count.invalid", 1);
            channel.close();
        }
        multiWriteRequest = mWrReqBuilder.build();
        if (multiWriteRequest.getRequestsCount() == 0) {
            RestServer.logger.debug("multi write no sub request");
            StatStore.getInstance().addCounter("rpc.multi-write.error.count", 1);
            channel.close();
            return;
        }
        StatStore.getInstance().addCounter("rpc.multi-write.count", 1);
        if (refCounter == null) {
            refCounter = new AtomicInteger(multiWriteRequest.getRequestsCount());
        } else {
            refCounter.set(multiWriteRequest.getRequestsCount());
        }
        if (clients == null) {
            clients = new LinkedList<AsyncClient>();
        } else {
            clients.clear();
        }
        if (multiWriteRequest.hasTimeout()) {
            requestTimeout = multiWriteRequest.getTimeout();
        }
        for (MessageProtos1.WriteRequest request : multiWriteRequest.getRequestsList()) {
            AsyncClient client = new AsyncClient(configuration);
            client.init();
            client.code = Status.kWriteRequest;
            client.subRequest = true;
            client.wrReq = request;
            client.parent = this;
            // sub request inherits from parent request.
            client.requestTimeout = requestTimeout;
            client.requestTimestamp = requestTimestamp;
            clients.add(client);
            CpuWorkerPool.getInstance().submit(client);
        }
    }

    public void readLocalCache() {
        RestServer.logger.debug("read local cache");

        if (readCacheQualifiers == null) {
            readCacheQualifiers = new ArrayList<String>();
        } else {
            readCacheQualifiers.clear();
        }

        // check local cache mean while fill the cache request.
        int readCount = 0;
        int cacheCount = 0;
        for (String q : rdReq.getQualifiersList()) {
            String cacheKey = null;
            byte[] b = null;
            if (configuration.isCache()) {
                cacheKey = makeCacheKey(prefix, q);
                RestServer.logger.debug("search cache with key = " + cacheKey);
                b = LocalCache.getInstance().get(cacheKey);
            }
            readCount += 1;
            if (b != null) {
                RestServer.logger.debug("cache hit!");
                cacheCount += 1;
                MessageProtos1.ReadResponse.KeyValue.Builder bd = MessageProtos1.ReadResponse.KeyValue.newBuilder();
                bd.setQualifier(q);
                bd.setContent(ByteString.copyFrom(b));
                rdRes.addKvs(bd);
            } else {
                RestServer.logger.debug("read hbase qualifier: " + q);
                readCacheQualifiers.add(q);
            }
        }
        StatStore.getInstance().addCounter("read.count", readCount);
        StatStore.getInstance().addCounter("read.count.local-cache", cacheCount);

        if (!readCacheQualifiers.isEmpty()) {
            code = Status.kReadHBaseService; // read cache service.
            readHBaseQualifiers = readCacheQualifiers;
        } else {
            code = Status.kReadResponse; // return directly.
        }
        run();
    }

    public void readHBaseService() {
        RestServer.logger.debug("read hbase service");

        final AsyncClient client = this;
        client.code = Status.kReadResponse;

        // already timeout, don't request hbase.
        if ((System.currentTimeMillis() - requestTimestamp) > requestTimeout) {
            RestServer.logger.debug("detect timeout before read request hbase");
            StatStore.getInstance().addCounter("read.count.timeout.before-request-hbase", 1);
            client.subRequestStatus = SubRequestStatus.kTimeout;
            // run in the same thread.
            client.run();
            return;
        }

        RestServer.logger.debug("tableName = " + tableName + ", rowKey = " + rowKey + ", columnFamily = " + columnFamily);
        GetRequest getRequest = new GetRequest(tableName, rowKey);
        getRequest.family(columnFamily);
        if (rdReq.getQualifiersCount() != 0) {
            // otherwise we read all qualifiers from column family.
            // a little bit tedious.
            byte[][] qualifiers = new byte[readHBaseQualifiers.size()][];
            int idx = 0;
            for (String q : readHBaseQualifiers) {
                qualifiers[idx] = q.getBytes();
                idx += 1;
            }
            getRequest.qualifiers(qualifiers);
            StatStore.getInstance().addCounter("read.count.hbase.column", qualifiers.length);
        } else {
            StatStore.getInstance().addCounter("read.count.hbase.column-family", 1);
        }

        client.readHBaseServiceStartTimestamp = System.currentTimeMillis();
        Deferred<ArrayList<KeyValue>> deferred = HBaseService.getInstance().get(getRequest);
        // if failed, we don't return.
        deferred.addCallback(new Callback<Object, ArrayList<KeyValue>>() {
            @Override
            public Object call(ArrayList<KeyValue> keyValues) throws Exception {
                // we don't return because we put into CpuWorkerPool.
                client.readHBaseServiceEndTimestamp = System.currentTimeMillis();
                if (client.rdReq.getQualifiersCount() != 0) {
                    // reorder qualifier according request qualifier order.
                    // then builder and the cache.
                    Map<String, KeyValue> mapping = new HashMap<String, KeyValue>();
                    for (KeyValue kv : keyValues) {
                        mapping.put(new String(kv.qualifier()), kv);
                    }
                    int missingCount = 0;
                    for (String k : rdReq.getQualifiersList()) {
                        KeyValue kv = mapping.get(k);
                        MessageProtos1.ReadResponse.KeyValue.Builder sub = MessageProtos1.ReadResponse.KeyValue.newBuilder();
                        sub.setQualifier(k);
                        byte[] v = kEmptyBytes;
                        if (kv == null) {
                            sub.setContent(ByteString.EMPTY);
                            missingCount++;
                        } else {
                            v = kv.value();
                            sub.setContent(ByteString.copyFrom(kv.value()));
                        }
                        rdRes.addKvs(sub.build());
                        // fill cache.
                        if (client.configuration.isCache()) {
                            String cacheKey = makeCacheKey(prefix, k);
                            RestServer.logger.debug("fill cache with key = " + cacheKey);
                            LocalCache.getInstance().set(cacheKey, v);
                        }
                    }
                    StatStore.getInstance().addCounter("read.count.field-not-exist", missingCount);
                    StatStore.getInstance().addCounter("read.duration.hbase.column",
                            client.readHBaseServiceEndTimestamp - client.readHBaseServiceStartTimestamp);
                } else {
                    // just fill the builder. don't save them to cache.
                    for (KeyValue kv : keyValues) {
                        String k = new String(kv.qualifier());
                        byte[] value = kv.value();
                        MessageProtos1.ReadResponse.KeyValue.Builder bd = MessageProtos1.ReadResponse.KeyValue.newBuilder();
                        bd.setQualifier(k);
                        bd.setContent(ByteString.copyFrom(value));
                        client.rdRes.addKvs(bd);
                    }
                    StatStore.getInstance().addCounter("read.duration.hbase.column-family",
                            client.readHBaseServiceEndTimestamp - client.readHBaseServiceStartTimestamp);
                }

                // already timeout
                if ((System.currentTimeMillis() - client.requestTimestamp) > client.requestTimeout) {
                    RestServer.logger.debug("detect timeout after read request hbase");
                    StatStore.getInstance().addCounter("read.count.timeout.after-request-hbase", 1);
                    client.subRequestStatus = SubRequestStatus.kTimeout;
                }

                // put back to CPU worker pool.
                CpuWorkerPool.getInstance().submit(client);
                return null;
            }
        });
        deferred.addErrback(new Callback<Object, Exception>() {
            @Override
            public Object call(Exception o) throws Exception {
                o.printStackTrace();
                StatStore.getInstance().addCounter("read.count.error", 1);
                client.subRequestStatus = SubRequestStatus.kException;
                CpuWorkerPool.getInstance().submit(client);
                return null;
            }
        });
    }

    public void writeHBaseService() {
        RestServer.logger.debug("write hbase service");

        final AsyncClient client = this;
        client.code = Status.kWriteResponse;

        // already timeout, don't request hbase.
        if ((System.currentTimeMillis() - requestTimestamp) > requestTimeout) {
            RestServer.logger.debug("detect timeout before write request hbase");
            StatStore.getInstance().addCounter("write.count.timeout.before-request-hbase", 1);
            client.subRequestStatus = SubRequestStatus.kTimeout;
            // run in the same thread.
            client.run();
            return;
        }

        RestServer.logger.debug("tableName = " + tableName + ", rowKey = " + rowKey + ", columnFamily = " + columnFamily);

        byte[][] qualifiers = new byte[wrReq.getKvsCount()][];
        byte[][] values = new byte[wrReq.getKvsCount()][];
        for (int i = 0; i < wrReq.getKvsCount(); i++) {
            qualifiers[i] = wrReq.getKvs(i).getQualifier().getBytes();
            values[i] = wrReq.getKvs(i).getContent().toByteArray();
        }
        StatStore.getInstance().addCounter("write.count", wrReq.getKvsCount());
        PutRequest putRequest = new PutRequest(tableName.getBytes(), rowKey.getBytes(), columnFamily.getBytes(), qualifiers, values);

        client.writeHBaseServiceStartTimestamp = System.currentTimeMillis();
        Deferred<Object> deferred = HBaseService.getInstance().put(putRequest);
        // if failed, we don't return.
        deferred.addCallback(new Callback<Object, Object>() {
            @Override
            public Object call(Object obj) throws Exception {
                // already timeout
                if ((System.currentTimeMillis() - client.requestTimestamp) > client.requestTimeout) {
                    RestServer.logger.debug("detect timeout after write request hbase");
                    StatStore.getInstance().addCounter("write.count.timeout.after-request-hbase", 1);
                    client.subRequestStatus = SubRequestStatus.kTimeout;
                }

                // we don't return because we put into CpuWorkerPool.
                CpuWorkerPool.getInstance().submit(client);
                return null;
            }
        });
        deferred.addErrback(new Callback<Object, Exception>() {
            @Override
            public Object call(Exception o) throws Exception {
                // we don't care.
                o.printStackTrace();
                StatStore.getInstance().addCounter("write.count.error", 1);
                client.subRequestStatus = SubRequestStatus.kException;
                CpuWorkerPool.getInstance().submit(client);
                return null;
            }
        });
    }

    public void readResponse() {
        if (subRequestStatus == SubRequestStatus.kOK) {
            readEndTimestamp = System.currentTimeMillis();
            StatStore.getInstance().addCounter("read.duration", readEndTimestamp - readStartTimestamp);
        }
        if (!subRequest) {
            if (subRequestStatus != SubRequestStatus.kOK) {
                channel.close();
                return;
            }
            msg = rdRes.build();
            code = Status.kHttpResponse;
            run();
        } else {
            int count = parent.refCounter.decrementAndGet();
            if (count == 0) {
                parent.mRdRes = MessageProtos1.MultiReadResponse.newBuilder();
                for (AsyncClient client : parent.clients) {
                    // if any one fails, then it fails.
                    if (client.subRequestStatus != SubRequestStatus.kOK) {
                        RestServer.logger.debug("read client status = " + client.subRequestStatus);
                        parent.channel.close();
                        return;
                    }
                    parent.mRdRes.addResponses(client.rdRes);
                }
                parent.msg = parent.mRdRes.build();
                parent.code = Status.kHttpResponse;
                CpuWorkerPool.getInstance().submit(parent);
            }
        }
    }

    public void writeResponse() {
        if (subRequestStatus == SubRequestStatus.kOK) {
            writeHBaseServiceEndTimestamp = System.currentTimeMillis();
            StatStore.getInstance().addCounter("write.duration",
                    writeHBaseServiceEndTimestamp - writeHBaseServiceStartTimestamp);
        }
        if (!subRequest) {
            if (subRequestStatus != SubRequestStatus.kOK) {
                channel.close();
                return;
            }
            msg = wrRes.build();
            code = Status.kHttpResponse;
            run();
        } else {
            int count = parent.refCounter.decrementAndGet();
            if (count == 0) {
                parent.mWrRes = MessageProtos1.MultiWriteResponse.newBuilder();
                for (AsyncClient client : parent.clients) {
                    if (client.subRequestStatus != SubRequestStatus.kOK) {
                        RestServer.logger.debug("write client status = " + client.subRequestStatus);
                        parent.channel.close();
                        return;
                    }
                    parent.mWrRes.addResponses(client.wrRes);
                }
                parent.msg = parent.mWrRes.build();
                parent.code = Status.kHttpResponse;
                CpuWorkerPool.getInstance().submit(parent);
            }
        }
    }

    public void handleHttpResponse() {
        RestServer.logger.debug("http response");

        HttpResponse response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        int size = msg.getSerializedSize();
        response.setHeader("Content-Length", size);
        ByteArrayOutputStream os = new ByteArrayOutputStream(size);
        try {
            msg.writeTo(os);
            ChannelBuffer buffer = ChannelBuffers.copiedBuffer(os.toByteArray());
            response.setContent(buffer);
            channel.write(response); // write over.
            StatStore.getInstance().addCounter("rpc.out.bytes", size);
        } catch (Exception e) {
            // just ignore it.
        }
    }
}
