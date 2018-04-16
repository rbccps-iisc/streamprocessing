package robertbosch.middleware.benchmarking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MiddlewareAPI {
	public static SmartcityDataSimulator ob;
	public static BufferedWriter pubwrtr;
	public static JSONParser parser = new JSONParser();
	
	public static void main(String[] args) throws Exception {
		ob = new SmartcityDataSimulator();
		String mode = "publish";
		if(mode.equals("publish")) {
			
			pubwrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/publish.txt")));
			int iterations =5;
			//long sleeptime =10;		//100 msg/sec
			String param = "light";
			int index=0;
			while(true) {
				
				doPost(param);
				index++;
				//Thread.sleep(sleeptime);
				if(iterations == index+1) {
					pubwrtr.close();
					break;
				}
			}
		}
	}
	
	private static void doGet() throws IOException, InterruptedException {
		String[] command = {"curl","-ik","-X","GET","https://13.59.139.227:10443/api/1.0.0/subscribe?name=streetlight", "-H", "apikey: 4d04c10f5bf24d4f8aa93529e0e07b07"};
		Process proc = Runtime.getRuntime().exec(command);
		int waitime = proc.waitFor();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String s;
		while((s=rdr.readLine()) != null) {
			System.out.println(s);
		}
		
		rdr.close();
		System.out.println("done");
	}
	
	private static void doPost(String param) throws IOException, InterruptedException, ParseException {
		String sensordata;
		if(param.equals("light")) {
			sensordata = ob.jsonstreetLight();
		} else {
			sensordata = ob.jsonenergyMeter();
		}
		
		Object ob = parser.parse(sensordata);
		JSONObject jsonob = (JSONObject)ob;
		
		pubwrtr.write(System.currentTimeMillis() + "," + jsonob.get("msgid") + "\n");
		String[] arr = {"curl","-ik","-X","POST","https://13.59.139.227:10443/api/1.0.0/publish","-H", "apikey",":", "4d04c10f5bf24d4f8aa93529e0e07b07","-d", "'{\"body\":\"" + sensordata + "\"}'"};
		//String[] arr = {"curl","-ik","-X","POST", "https://13.59.139.227:10443/api/1.0.0/publish", "-H", "'apikey: 4d04c10f5bf24d4f8aa93529e0e07b07'  -d '{\"body\":\"" + sensordata + "\"}'"};
		//String[] arr = {"curl","-ik","-X","POST", "https://13.59.139.227:10443/api/1.0.0/publish", "-H", "'apikey: 4d04c10f5bf24d4f8aa93529e0e07b07'", "-d", "'{\"body\":\""+ sensordata + "\"}\"}'"};
		//Process proc = Runtime.getRuntime().exec(arr);
		//int waitime = proc.waitFor();
		
		ProcessBuilder builder = new ProcessBuilder("curl","-ik","-X","POST","https://13.59.139.227:10443/api/1.0.0/publish","-H", "apikey",":", "4d04c10f5bf24d4f8aa93529e0e07b07","-d", "'{\"body\":\"" + sensordata + "\"}'");
		Process proc = builder.start();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String s;
		while((s=rdr.readLine()) != null) {
			System.out.println(s);
		}
		System.out.println("done");
		
	}
}
