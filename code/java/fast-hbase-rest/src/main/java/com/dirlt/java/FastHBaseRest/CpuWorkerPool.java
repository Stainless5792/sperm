package com.dirlt.java.FastHBaseRest;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 1:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class CpuWorkerPool {
    // singleton.
    private ExecutorService executorService = null;
    private static CpuWorkerPool instance = null;

    public static void init(Configuration configuration) {
        instance = new CpuWorkerPool(configuration);
    }

    private CpuWorkerPool(Configuration configuration) {
        executorService = new ThreadPoolExecutor(configuration.getCpuThreadNumber(), configuration.getCpuThreadNumber(),
                0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(configuration.getCpuQueueSize()));
    }

    public static CpuWorkerPool getInstance() {
        return instance;
    }

    public Future submit(Runnable runnable) {
        // wait runnable.
        return executorService.submit(runnable);
    }
}
