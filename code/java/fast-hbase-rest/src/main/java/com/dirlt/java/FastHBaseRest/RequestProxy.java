package com.dirlt.java.FastHBaseRest;

import com.dirlt.java.FastHbaseRest.MessageProtos1;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 6/20/13
 * Time: 9:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class RequestProxy {
    private Configuration configuration;
    private static RequestProxy instance = null;

    private RequestProxy(Configuration configuration) {
        this.configuration = configuration;
    }

    public static void init(Configuration configuration) {
        instance = new RequestProxy(configuration);
    }

    public static RequestProxy getInstance() {
        return instance;
    }

    public MessageProtos1.ReadRequest handleReadRequest(MessageProtos1.ReadRequest readRequest) throws Exception {
        RestServer.logger.debug("proxy handle read request");
        if (readRequest.getTableName().equals("appuserstat")) {
            String rowKey = readRequest.getRowKey();
            String date = rowKey.split("_")[0];
            if (date.matches("^\\d{4}-\\d{2}-\\d{2}$") && date.compareTo(configuration.getKv().get("appuserstat.date.addHashCodeAsRowKeyPrefix")) >= 0) {
                MessageProtos1.ReadRequest.Builder builder = MessageProtos1.ReadRequest.newBuilder(readRequest);
                builder.setRowKey(Utility.addHashCodeAsPrefix(rowKey));
                return builder.build();
            }
        }
        return readRequest;
    }

    public MessageProtos1.WriteRequest handleWriteRequest(MessageProtos1.WriteRequest writeRequest) throws Exception {
        RestServer.logger.debug("proxy handle write request");
        return writeRequest;
    }
}
