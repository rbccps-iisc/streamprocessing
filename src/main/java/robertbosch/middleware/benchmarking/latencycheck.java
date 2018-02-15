package robertbosch.middleware.benchmarking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class latencycheck {
	public static void main(String[] args) throws IOException {
		String sub = "/Users/sahiltyagi/Desktop/subscribe.txt";
		String lat = "/Users/sahiltyagi/Desktop/latency_out.txt";
		String pub = "/Users/sahiltyagi/Desktop/publish.txt";
		
//		String sub = "/home/etl_subsystem/subscribe.txt";
//		String lat = "/home/etl_subsystem/latency_out.txt";
//		String pub = "/home/etl_subsystem/publish.txt";
		int ctr=0;
		long sumlatency=0L;
		
		BufferedReader subrdr = new BufferedReader(new InputStreamReader(new FileInputStream(sub)));
		BufferedReader pubrdr = new BufferedReader(new InputStreamReader(new FileInputStream(pub)));
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(lat)));
		String rec;
		Map<String, Long> submap = new HashMap<String, Long>();
		Map<String, Long> pubmap = new HashMap<String, Long>();
		
		while((rec=subrdr.readLine()) != null) {
			submap.put(rec.split(",")[1].trim(), Long.parseLong(rec.split(",")[0]));
		}
		subrdr.close();
		
		while((rec=pubrdr.readLine()) != null) {
			pubmap.put(rec.split(",")[1].trim(), Long.parseLong(rec.split(",")[0]));
		}
		pubrdr.close();
		System.out.println("pubmap size: " + pubmap.size());
		System.out.println("submap size: " + submap.size());
		
		for(Map.Entry<String, Long> set : pubmap.entrySet()) {
			String k = set.getKey();
			System.out.println(k);
			if(submap.containsKey(k)) {
				long val = submap.get(k) - set.getValue();
				output.write(String.valueOf(val) + "\n");
				sumlatency +=val;
				ctr++;
			}
		}
		output.close();
		System.out.println("counter is: " + ctr);
		
		System.out.println("total latency incurred:" + Double.parseDouble(String.valueOf(sumlatency/ctr)));
	}
}
