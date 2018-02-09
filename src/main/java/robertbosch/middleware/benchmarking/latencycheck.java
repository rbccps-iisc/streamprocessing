package robertbosch.middleware.benchmarking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class latencycheck {
	public static void main(String[] args) throws IOException {
//		String sub = "/Users/sahiltyagi/Desktop/subscribe.txt";
//		String lat = "/Users/sahiltyagi/Desktop/latency_out.txt";
//		String pub = "/Users/sahiltyagi/Desktop/publish.txt";
		
		String sub = "/home/etl_subsystem/subscribe.txt";
		String lat = "/home/etl_subsystem/latency_out.txt";
		String pub = "/home/etl_subsystem/publish.txt";
		int ctr=0;
		long sumlatency=0L;
		
		BufferedReader subrdr = new BufferedReader(new InputStreamReader(new FileInputStream(sub)));
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(lat)));
		String rec;
		while((rec=subrdr.readLine()) != null) {
			BufferedReader pubrdr = new BufferedReader(new InputStreamReader(new FileInputStream(pub)));
			String line;
			while((line=pubrdr.readLine()) !=null) {
				if(rec.split(",")[1].equalsIgnoreCase(line.split(",")[1])) {
					System.out.println(rec.split(",")[1] + "    " + line.split(",")[1]);
					long val = (Long.parseLong(rec.split(",")[0]) - Long.parseLong(line.split(",")[0]));
					if(val>0) {
						sumlatency +=val;
						ctr++;
					}
					output.write(String.valueOf(Long.parseLong(rec.split(",")[0]) - Long.parseLong(line.split(",")[0])) + "\n");
				}
			}
			pubrdr.close();
		}
		
		System.out.println("total latency incurred: " + Double.parseDouble(String.valueOf(sumlatency/ctr)));
		subrdr.close();
		output.close();
	}
}
