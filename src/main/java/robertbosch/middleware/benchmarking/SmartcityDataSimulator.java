package robertbosch.middleware.benchmarking;

import java.io.IOException;
import java.util.UUID;
//import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.json.simple.JSONObject;

//import com.protoTest.smartcity.Pollut;
//import com.protoTest.smartcity.Sensed;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import robertbosch.utils.RobertBoschUtils;

public class SmartcityDataSimulator {
	JSONObject data = null;
	
	private void jsonstreetLight() {
		
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
		
		String deviceId = "streetlight-" + UUID.randomUUID().toString();
		
		String packet = "[\"key\": \"" + deviceId + "\"," + data.toJSONString() + "]";
		System.out.println(packet);
		
//		try {
//			RobertBoschUtils.publishchannel.queueDeclare("simulator", false, false, false, null);
//			RobertBoschUtils.publishchannel.basicPublish("", RobertBoschUtils.pubTopic, null, packet.getBytes());
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
	}
	
	private void protostreetlight() {
		
		boolean[] state = {true, false};
		int luxOutput = ThreadLocalRandom.current().nextInt(100, 1001);
		int powerconsumption = ThreadLocalRandom.current().nextInt(0, 101);
		int casetemperature = ThreadLocalRandom.current().nextInt(1, 101);
		int ambientlux = ThreadLocalRandom.current().nextInt(100, 1001);
		boolean slaveAlive = state[ThreadLocalRandom.current().nextInt(0,2)];
		int batterylevel = ThreadLocalRandom.current().nextInt(0, 5001);
		int dataSamplingInstant = ThreadLocalRandom.current().nextInt(10000000, 99999999);
		
//		Sensed.sensor_values.Builder sensorval = Sensed.sensor_values.newBuilder();
//		sensorval.setLuxOutput(luxOutput);
//		sensorval.setPowerConsumption(powerconsumption);
//		sensorval.setCaseTemperature(casetemperature);
//		sensorval.setAmbientLux(ambientlux);
//		sensorval.setSlaveAlive(slaveAlive);
//		sensorval.setBatteryLevel(batterylevel);
//		sensorval.setDataSamplingInstant(dataSamplingInstant);
		
	}
	
	private void protopollution() {
		
		int pm25 = ThreadLocalRandom.current().nextInt(100, 1001);
		int pm10 = ThreadLocalRandom.current().nextInt(100, 1001);
		int co2 = ThreadLocalRandom.current().nextInt(10, 50);
		float noiselevel = ThreadLocalRandom.current().nextInt(0, 100);
		
//		Pollut.pollution.Builder pollutiondata = Pollut.pollution.newBuilder();
//		pollutiondata.setPM25(pm25);
//		pollutiondata.setPM10(pm10);
//		pollutiondata.setCO2(co2);
//		pollutiondata.setNOISELEVEL(noiselevel);
		
	}
	
	private void jsonenergyMeter() {
		
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
		
		String deviceId = "energy-" + UUID.randomUUID().toString();
		
		//String packet = "[\"key\": \"energymeter_id\"," + data.toJSONString() + "]";
		String packet = "[\"key\": \"" + deviceId + "\"," + data.toJSONString() + "]";
		System.out.println(packet);
	}
	
	private static Channel createbrokerChannel(String deviceId) {
		
		ConnectionFactory connfac = new ConnectionFactory();
		connfac.setHost("10.156.14.6");
		connfac.setPort(5672);
		connfac.setUsername("rbccps");
		connfac.setPassword("rbccps@123");
		Channel channel = null;
		
		try {
			Connection conn = connfac.newConnection();
			channel = conn.createChannel();
			channel.queueDeclare(deviceId, false, false, false, null);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException t) {
			t.printStackTrace();
		}
		return channel;
	}
	
	public static void main(String[] args) {
		//RobertBoschUtils.getPublishChannel();
		SmartcityDataSimulator obj = new SmartcityDataSimulator();
		int iterations =10;
		int index=0;
		while(index<iterations) {
			obj.jsonstreetLight();
			//obj.jsonenergyMeter();
			index++;
		}
		
		System.out.println("complete.");
	}
}
