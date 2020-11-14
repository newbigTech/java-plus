//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.SynchronousQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class Test {
//
//	public static void main(String[] args) {
//		 Map<String, AtomicInteger> inDegreeAtomicIntMap=new HashMap<String, AtomicInteger>();
//		 inDegreeAtomicIntMap.put("1", new AtomicInteger(1));
//		 inDegreeAtomicIntMap.put("2", new AtomicInteger(200));
//		 inDegreeAtomicIntMap.put("3", new AtomicInteger(300));
//		 System.out.println(inDegreeAtomicIntMap);
//		 
//		 inDegreeAtomicIntMap.get("1").decrementAndGet();
//		 inDegreeAtomicIntMap.get("1").decrementAndGet();
//		 inDegreeAtomicIntMap.get("2").decrementAndGet(); 
//
//			 System.out.println(inDegreeAtomicIntMap);
//	}
//	
//    private final static ExecutorService executorPool = new ThreadPoolExecutor(50, 500, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));
//    
//	
//    public final static ThreadPoolExecutor EXECUTOR_POOL = new ThreadPoolExecutor(20, Integer.parseInt("600"),
//            60, TimeUnit.SECONDS, new SynchronousQueue<>(), new DefaultThreadFactory("coreThreadPool-"),
//            new ThreadPoolExecutor.CallerRunsPolicy());
//
//}
