package robertbosch.middleware.benchmarking;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.json.simple.JSONObject;

import robertbosch.utils.RobertBoschUtils;

public class StreetlightSimulator {
	JSONObject data = null;
	
	private void streetLightData() {
		
		boolean[] state = {true, false};
		int luxOutput = ThreadLocalRandom.current().nextInt(100, 1001);
		int powerconsumption = ThreadLocalRandom.current().nextInt(0, 101);
		int casetemperature = ThreadLocalRandom.current().nextInt(1, 101);
		int ambientlux = ThreadLocalRandom.current().nextInt(100, 1001);
		boolean slaveAlive = state[ThreadLocalRandom.current().nextInt(0,2)];
		int batterylevel = ThreadLocalRandom.current().nextInt(0, 5001);
		int dataSamplingInstant = ThreadLocalRandom.current().nextInt(10000000, 99999999);
		
		data = new JSONObject();
		data.put("luxOutput", luxOutput);
		data.put("powerConsumption", powerconsumption);
		data.put("caseTemperature", casetemperature);
		data.put("ambientLux", ambientlux);
		data.put("slaveAlive", slaveAlive);
		data.put("batteryLevel", batterylevel);
		data.put("dataSamplingInstant", dataSamplingInstant);
		
		String packet = "[\"key\": \"streetlight_id\"," + data.toJSONString() + "]";
		System.out.println(packet);
		
		try {
			RobertBoschUtils.publishchannel.queueDeclare("simulator", false, false, false, null);
			RobertBoschUtils.publishchannel.basicPublish("", RobertBoschUtils.pubTopic, null, packet.getBytes());
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		RobertBoschUtils.getPublishChannel();
		StreetlightSimulator obj = new StreetlightSimulator();
		int iterations =1000;
		int index=0;
		while(index<iterations) {
			obj.streetLightData();
			index++;
		}
	}
}
