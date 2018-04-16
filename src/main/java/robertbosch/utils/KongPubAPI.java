package robertbosch.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KongPubAPI {
	JSONObject data = null;
	
	public static void main(String[] args) {
		JSONParser parser = new JSONParser();
		Object obj=null;
		String msgid, sensordata;
		JSONObject jsonob=null;
		KongPubAPI ob = new KongPubAPI();
		
		int itr=10,index=0;
		
		BufferedWriter writer=null;
		try {
			writer= new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/publish10000.txt")));
		} catch(IOException io) {
			io.printStackTrace();
		}	
		
		while(index<itr) {
			sensordata = ob.streetlight();
			//sensordata = ob.energymeter();
			try {
				obj = parser.parse(sensordata);
				jsonob = (JSONObject)obj;
				
				String str = jsonob.get("body").toString();
				obj = parser.parse(str);
				jsonob = (JSONObject)obj;
				//System.out.println("message id: " + jsonob.get("msgid"));
			} catch(ParseException p) {
				p.printStackTrace();
			}
			
			//System.out.println(sensordata);
			ProcessBuilder builder = new ProcessBuilder("/Users/sahiltyagi/Desktop/ideampublish.sh", sensordata);
//			ProcessBuilder builder = new ProcessBuilder("/usr/bin/curl","-ik","-X","POST","https://52.14.204.82:10443/api/1.0.0/publish","-H",
//									"'apikey",":","9b3ef67227754c839d8f8386f21201b1'","-d", sensordata);
			try {
				writer.write(System.currentTimeMillis() + "," + jsonob.get("msgid") + "\n");
				Process proc = builder.start();
				//System.out.println("published...");
				
				BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));
				String s;
				while((s=rdr.readLine()) != null) {
					System.out.println(s);
				}
			} catch(IOException io) {
				io.printStackTrace();
			}
			
			index++;
			
			//input rate=10 msg/sec
			try {
				Thread.sleep(100);
			} catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			writer.close();
			System.out.println("publishing complete...");
		} catch(IOException io) {
			io.printStackTrace();
		}
	}
	
	private String streetlight() {
		boolean[] state = {true, false};
		int luxOutput = ThreadLocalRandom.current().nextInt(100, 1001);
		int powerconsumption = ThreadLocalRandom.current().nextInt(0, 101);
		int casetemperature = ThreadLocalRandom.current().nextInt(1, 101);
		int ambientlux = ThreadLocalRandom.current().nextInt(100, 1001);
		boolean slaveAlive = state[ThreadLocalRandom.current().nextInt(0,2)];
		int batterylevel = ThreadLocalRandom.current().nextInt(0, 5001);
		int dataSamplingInstant = ThreadLocalRandom.current().nextInt(10000000, 99999999);
		
		data = new JSONObject();
		//data.put("devEUI", "streetlight-" + UUID.randomUUID().toString());
		data.put("luxOutput", luxOutput);
		data.put("powerConsumption", powerconsumption);
		data.put("caseTemperature", casetemperature);
		data.put("ambientLux", ambientlux);
		data.put("slaveAlive", slaveAlive);
		data.put("batteryLevel", batterylevel);
		data.put("dataSamplingInstant", dataSamplingInstant);
		
		String msgId = "streetlight-" + UUID.randomUUID().toString();
		data.put("msgid", msgId);
		
		JSONObject finalobj = new JSONObject();
		finalobj.put("body", data.toJSONString());
		
		return finalobj.toJSONString();
	}
	
	private String energymeter() {
		double YPhaseReactivePower = ThreadLocalRandom.current().nextDouble(5, 30);
		double YPhaseApparentPower = ThreadLocalRandom.current().nextDouble(10, 30);
		double YPhaseActivePower = ThreadLocalRandom.current().nextDouble(-10, 10);
		double BPhaseVoltage = ThreadLocalRandom.current().nextDouble(100, 400);
		double RPhasePowerFactor = ThreadLocalRandom.current().nextDouble(-1, 1);
		double BPhaseActivePower = ThreadLocalRandom.current().nextDouble(10, 30);
		double EnergyReactive = ThreadLocalRandom.current().nextDouble(10000, 30000);
		double BPhaseCurrent = ThreadLocalRandom.current().nextDouble(0, 1);
		double RPhaseApparentPower = ThreadLocalRandom.current().nextDouble(1000, 5000);
		double RPhaseReactivePower = ThreadLocalRandom.current().nextDouble(100, 1000);
		double YPhasePowerFactor = ThreadLocalRandom.current().nextDouble(-1, 1);
		double RPhaseVoltage = ThreadLocalRandom.current().nextDouble(100, 400);
		double BPhaseReactivePower = ThreadLocalRandom.current().nextDouble(5, 30);
		double BPhasePowerFactor = ThreadLocalRandom.current().nextDouble(-1, 1);
		double RPhaseActivePower = ThreadLocalRandom.current().nextDouble(100, 1500);
		double YPhaseCurrent = ThreadLocalRandom.current().nextDouble(0, 1);
		double YPhaseVoltage = ThreadLocalRandom.current().nextDouble(100, 400);
		double RPhaseCurrent = ThreadLocalRandom.current().nextDouble(1, 10);
		double BPhaseApparentPower = ThreadLocalRandom.current().nextDouble(10, 30);
		int dataSamplingInstant = ThreadLocalRandom.current().nextInt(10000000, 99999999);
		double EnergyActive = ThreadLocalRandom.current().nextDouble(10000, 30000);
		
		data = new JSONObject();
		data.put("YPhaseReactivePower", YPhaseReactivePower);
		data.put("YPhaseApparentPower", YPhaseApparentPower);
		data.put("YPhaseActivePower", YPhaseActivePower);
		data.put("BPhaseVoltage", BPhaseVoltage);
		data.put("RPhasePowerFactor", RPhasePowerFactor);
		data.put("BPhaseActivePower", BPhaseActivePower);
		data.put("EnergyReactive", EnergyReactive);
		data.put("BPhaseCurrent", BPhaseCurrent);
		data.put("RPhaseApparentPower", RPhaseApparentPower);
		data.put("RPhaseReactivePower", RPhaseReactivePower);
		data.put("YPhasePowerFactor", YPhasePowerFactor);
		data.put("RPhaseVoltage", RPhaseVoltage);
		data.put("BPhaseReactivePower", BPhaseReactivePower);
		data.put("BPhasePowerFactor", BPhasePowerFactor);
		data.put("RPhaseActivePower", RPhaseActivePower);
		data.put("YPhaseCurrent", YPhaseCurrent);
		data.put("YPhaseVoltage", YPhaseVoltage);
		data.put("RPhaseCurrent", RPhaseCurrent);
		data.put("BPhaseApparentPower", BPhaseApparentPower);
		data.put("dataSamplingInstant", dataSamplingInstant);
		data.put("EnergyActive", EnergyActive);
		
		String msgid = "energy-" + UUID.randomUUID().toString();
		data.put("msgid", msgid);
		
		JSONObject finalobj = new JSONObject();
		finalobj.put("body", data.toJSONString());
		
		return finalobj.toJSONString();
	}
}
