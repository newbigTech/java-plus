import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {

	public static void main(String[] args) {
		 Map<String, AtomicInteger> inDegreeAtomicIntMap=new HashMap<String, AtomicInteger>();
		 inDegreeAtomicIntMap.put("1", new AtomicInteger(1));
		 inDegreeAtomicIntMap.put("2", new AtomicInteger(200));
		 inDegreeAtomicIntMap.put("3", new AtomicInteger(300));
		 System.out.println(inDegreeAtomicIntMap);
		 
		 inDegreeAtomicIntMap.get("1").decrementAndGet();
		 inDegreeAtomicIntMap.get("1").decrementAndGet();
		 inDegreeAtomicIntMap.get("2").decrementAndGet(); 

			 System.out.println(inDegreeAtomicIntMap);
	}

}
