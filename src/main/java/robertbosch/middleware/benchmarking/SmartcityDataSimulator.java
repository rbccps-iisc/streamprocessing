package robertbosch.middleware.benchmarking;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;
//import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;

import com.google.protobuf.util.JsonFormat;
//import com.protoTest.smartcity.Pollut;
import com.protoTest.smartcity.Sensed;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import robertbosch.utils.RobertBoschUtils;

public class SmartcityDataSimulator implements MqttCallback {
	JSONObject data = null;
	static Channel channel;
	static BufferedWriter publish;
	static MqttMessage mqttpub = new MqttMessage();
	
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
		//data.put("devEUI", "streetlight-" + UUID.randomUUID().toString());
		data.put("luxOutput", luxOutput);
		data.put("powerConsumption", powerconsumption);
		data.put("caseTemperature", casetemperature);
		data.put("ambientLux", ambientlux);
		data.put("slaveAlive", slaveAlive);
		data.put("batteryLevel", batterylevel);
		data.put("dataSamplingInstant", dataSamplingInstant);
		
		String msgId = "streetlight-" + UUID.randomUUID().toString();
		
		JSONObject finalobj = new JSONObject();
		finalobj.put("msgid", msgId);
		finalobj.put("data", data);
		
		String packet = finalobj.toJSONString();
		System.out.println("size of packets: " + packet.getBytes().length);
		
		try {
			String topic = "sahil";
			channel.basicPublish("", topic, null, packet.getBytes());
			publish.write(System.currentTimeMillis() + "," + msgId + "\n");
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	private void protostreetlight() throws Exception {
		
		SmartcityDataSimulator simulator = new SmartcityDataSimulator();
		boolean[] state = {true, false};
		int luxOutput = ThreadLocalRandom.current().nextInt(100, 1001);
		int powerconsumption = ThreadLocalRandom.current().nextInt(0, 101);
		int casetemperature = ThreadLocalRandom.current().nextInt(1, 101);
		int ambientlux = ThreadLocalRandom.current().nextInt(100, 1001);
		boolean slaveAlive = state[ThreadLocalRandom.current().nextInt(0,2)];
		int batterylevel = ThreadLocalRandom.current().nextInt(0, 5001);
		int dataSamplingInstant = ThreadLocalRandom.current().nextInt(10000000, 99999999);
		
		Sensed.sensor_values.Builder sensorval = Sensed.sensor_values.newBuilder();
		sensorval.setLuxOutput(luxOutput);
		sensorval.setPowerConsumption(powerconsumption);
		sensorval.setCaseTemperature(casetemperature);
		sensorval.setAmbientLux(ambientlux);
		sensorval.setSlaveAlive(slaveAlive);
		sensorval.setBatteryLevel(batterylevel);
		sensorval.setDataSamplingInstant(dataSamplingInstant);
		
		byte[] snsr = sensorval.build().toByteArray();
		JSONObject ob = new JSONObject();
		String deviceId = UUID.randomUUID().toString();
		ob.put("devEUI", deviceId);
		ob.put("data", snsr);
		String packet = ob.toJSONString();
		
//		System.out.println(ob.toJSONString());
//		Object o = Sensed.sensor_values.parseFrom(snsr);
//		String packet=JsonFormat.printer().print((Sensed.sensor_values)o);
//		System.out.println("packet is:" + packet);
		
		
//		System.out.println("protostreet light size:" + ob.toJSONString().getBytes().length);
//		simulator.publishToNetworkServer(ob.toJSONString().getBytes());
		
		try {
			String topic = "sahil";
			channel.basicPublish("", topic, null, packet.getBytes());
			publish.write(System.currentTimeMillis() + "," + deviceId + "\n");
			
		} catch(IOException e) {
			e.printStackTrace();
		}
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
//		
//		byte[] snsr = pollutiondata.build().toByteArray();
//		JSONObject ob = new JSONObject();
//		ob.put("devEUI", "70b3d58ff0031f00");
//		ob.put("data", snsr);
//		System.out.println("pollution sensor size: " + ob.toJSONString().getBytes().length);
		
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
		
//		String packet = "[\"key\": \"" + deviceId + "\"," + data.toJSONString() + "]";
//		System.out.println("energy meter size: " + packet.getBytes().length);
		
		JSONObject finalobj = new JSONObject();
		finalobj.put("devEUI", deviceId);
		finalobj.put("data", data);
		String packet = finalobj.toJSONString();
		try {
			String topic = "sahil";
			channel.basicPublish("", topic, null, packet.getBytes());
			publish.write(System.currentTimeMillis() + "," + deviceId + "\n");
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	private Channel createbrokerChannel(String deviceId) {
		
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
	
	private void publishToNetworkServer(byte[] data) {
		MqttConnectOptions connection = new MqttConnectOptions();
		connection.setAutomaticReconnect(true);
		connection.setCleanSession(false);
		connection.setConnectionTimeout(30);
		connection.setUserName("loraserver");
		connection.setPassword("loraserver".toCharArray());
		
		try {
			MqttClient client = new MqttClient("tcp://gateways.rbccps.org:1883", MqttClient.generateClientId());
			client.setCallback(this);
			client.connect(connection);
			
			mqttpub = new MqttMessage();
			mqttpub.setQos(2);
			mqttpub.setPayload(data);
			client.publish("sahil1", mqttpub);
			
		} catch(MqttException m) {
			m.printStackTrace();
		}

	}
	
	public static void main(String[] args) throws Exception {
		
		//simulator emitting a single device
		SmartcityDataSimulator obj = new SmartcityDataSimulator();
		channel = obj.createbrokerChannel("sahil");
		String publishfile = "/home/etl_subsystem/publish.txt";
		publish = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(publishfile)));
		
		int iterations =Integer.parseInt(args[0]);
		long sleeptime =Long.parseLong(args[1]);
		int index=0;
		while(index<iterations) {
			obj.jsonstreetLight();
			index++;
			Thread.sleep(sleeptime);
		}
		
		publish.close();
		System.out.println("complete.");
		
	}

	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
