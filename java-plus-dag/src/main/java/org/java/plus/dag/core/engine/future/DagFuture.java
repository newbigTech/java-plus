package org.java.plus.dag.core.engine.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author seven.wxy
 * @date 2019/3/26
 */
public class DagFuture<T> extends CompletableFuture {
    private Future<T> realFuture;
    private long startTime;
    private long waitTimeOutMs;
    private long realWaitTimeOutMs = 0L;

    private DagFuture() {
    }

    public DagFuture(Future<T> future) {
        this.realFuture = future;
        this.startTime = System.currentTimeMillis();
    }

    public DagFuture(Future<T> future, long waitTimeOutMs) {
        this.realFuture = future;
        this.startTime = System.currentTimeMillis();
        this.waitTimeOutMs = waitTimeOutMs;
    }

    public void setWaitTimeOutMs(long waitTimeOutMs) {
        this.waitTimeOutMs = waitTimeOutMs;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.realFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return this.realFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.realFuture.isDone();
    }

    @Override
    public T get() throws ExecutionException, InterruptedException {
        try {
            return this.get(waitTimeOutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException("Future execute timeout " + realWaitTimeOutMs, e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        long alreadyCostTimeMs = System.currentTimeMillis() - this.startTime;
        long realWaitTimeOutMs = timeout - alreadyCostTimeMs;
        if (realWaitTimeOutMs <= 0L) {
            realWaitTimeOutMs = 1L;
        }
        this.realWaitTimeOutMs = realWaitTimeOutMs;
        return this.realFuture.get(realWaitTimeOutMs, unit);
    }

    public long getRealWaitTimeOutMs() {
        return this.realWaitTimeOutMs;
    }

    public static <T> DagFuture<T> completed(final T result) {
        DagFuture<T> dagFuture = new DagFuture(new Future<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                return result;
            }

            @Override
            public T get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
                return result;
            }
        });
        return dagFuture;
    }
}
