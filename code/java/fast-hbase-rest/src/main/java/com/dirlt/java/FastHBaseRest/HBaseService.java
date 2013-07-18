package com.dirlt.java.FastHBaseRest;

import com.stumbleupon.async.Deferred;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 5:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseService {
    // singleton.
    private static HBaseService instance = null;
    private HBaseClient client = null;

    public static void init(Configuration configuration) {
        instance = new HBaseService(configuration);
    }

    private HBaseService(Configuration configuration) {
        client = new HBaseClient(configuration.getQuorumSpec());
    }

    public static HBaseService getInstance() {
        return instance;
    }

    // simple wrapper.
    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        return client.get(request);
    }

    public Deferred<Object> put(PutRequest putRequest) {
        return client.put(putRequest);
    }
}
