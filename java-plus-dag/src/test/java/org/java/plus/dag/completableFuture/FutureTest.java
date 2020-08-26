package org.java.plus.dag.completableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
 
/**
 * @author seven.wxy
 * @date 2019/6/3
 */
public class FutureTest  {
 
    public static void main(String[] args) throws Exception{
        CompletableFuture future0 = CompletableFuture.completedFuture(234);
        CompletableFuture future1 = future0.handleAsync((un, b) -> fun(future0), MoreExecutors.directExecutor());
        System.out.println(future1.get(500, TimeUnit.MILLISECONDS));

        //SettableFuture settableFuture = SettableFuture.create();
        //settableFuture.addListener(()-> fun(settableFuture), MoreExecutors.sameThreadExecutor());
        //System.out.println(settableFuture.get(500000000, TimeUnit.MILLISECONDS));

        //SettableFuture settableFuture = SettableFuture.create();
        //System.out.println(settableFuture.get(500000000, TimeUnit.MILLISECONDS));

        //LinkedTransferQueue<Object> queue = new LinkedTransferQueue<>();
        //System.out.println(queue.poll(500000000, TimeUnit.MILLISECONDS));

        //CompletableFuture future = new CompletableFuture();
        //System.out.println(future.get(500000000, TimeUnit.MILLISECONDS));
        CompletableFuture future = CompletableFuture.completedFuture(0);
        System.out.println("main thread id=" + Thread.currentThread().getId());
        future = future
            .handleAsync((a,b) -> {System.out.println(1);sleep(1000);System.out.println("A thread id=" + Thread.currentThread().getId());return 1;})
            .handleAsync((a,b) -> {System.out.println(2);sleep(1000);System.out.println("B thread id=" + Thread.currentThread().getId());return 2;});
        try {
            future.get(2102, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            //future.cancel(true);
        }
    }

    public static Object sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e){
            e.printStackTrace();
        }
        return ms;
    }

    public static Object fun(SettableFuture future){
        try {
            Thread.sleep(1000);
            future.set(234);
        } catch (Exception e){}
        return 123;
    }

    public static Object fun(CompletableFuture future){
        try {
            Thread.sleep(400);
        } catch (Exception e){}
        future.complete(new Object());
        return 123;
    }
}
