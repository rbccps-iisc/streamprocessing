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
		BufferedReader subrdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/subscribe.txt")));
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/latency_out.txt")));
		String rec;
		while((rec=subrdr.readLine()) != null) {
			BufferedReader pubrdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/publish.txt")));
			String line;
			while((line=pubrdr.readLine()) !=null) {
				if(rec.split(",")[1].equalsIgnoreCase(line.split(",")[1])) {
					System.out.println(rec.split(",")[1] + "    " + line.split(",")[1]);
					output.write(String.valueOf(Long.parseLong(rec.split(",")[0]) - Long.parseLong(line.split(",")[0])) + "\n");
				}
			}
			pubrdr.close();
		}
		
		subrdr.close();
		output.close();
	}
}