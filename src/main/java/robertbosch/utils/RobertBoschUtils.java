package robertbosch.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.bson.Document;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RobertBoschUtils {
//	public static String brokerhost = "10.156.14.9";
//	public static int brokerPort = 5672;
//	public static String brokerUsername = "rbccps";
//	public static String brokerPassword = "rbccps@123";
//	public static String brokerVHost = "rbccps_vhost";
//	public static String EXCHANGE_NAME = "rbccps_iot";
//	public static String bindingKey = "*.#";
	public static ConcurrentHashMap<String, String> catalogue = new ConcurrentHashMap<String, String>();
	
	public static String brokerhost = "localhost";
	public static int brokerPort = 5672;
	
	public static void main(String[] args) throws IOException {
//		ConnectionFactory factory = new ConnectionFactory();
//		factory.setHost(brokerhost);
//		factory.setPort(brokerPort);
//		try {
//			Connection conn = factory.newConnection();
//			//subscribeToBroker("t2");
//			publishToBroker("t1");
//			
//		} catch(IOException e) {
//			e.printStackTrace();
//		} catch(TimeoutException timeout) {
//			timeout.printStackTrace();
//		}
		
//		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("C:/Users/Sahil Tyagi/Desktop/ad hoc data/schema.json")));
//		String str=null;
//		StringBuilder schemabuilder = new StringBuilder();
//		while((str=rdr.readLine()) != null) {
//			schemabuilder.append(str);
//		}
//		rdr.close();
//		
//		String data = "{\"caseTemperature\": 33,\"powerConsumption\": 145,\"ambientLux\": 10,\"targetPowerState\": \"ON\",\"targetControlPolicy\": \"AUTO_TIMER\","
//				+ "\"targetAutoTimerParams\": { \"targetOnTime\": 343434, \"targetOffTime\": 6767676}}";
//		
//		boolean val = validateSchema(schemabuilder.toString(), data);
//		if(val) {
//			System.out.println("yes");
//		} else {
//			System.out.println("no");
//		}
		
		establishCatalogueDBConn();
		
	}
	
	public static boolean validateSchema(String schema, String data) {
		boolean status=false;
		try {
			JsonNode node = JsonLoader.fromString(data);
			JsonNode schemanode = JsonLoader.fromString(schema);
			JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
			JsonSchema jsonschema = factory.getJsonSchema(schemanode);
			
			ProcessingReport proc = jsonschema.validate(node);
			status = proc.isSuccess();
			
		} catch(IOException io) {
			io.printStackTrace();
		} catch(ProcessingException p) {
			p.printStackTrace();
		}
		
		return status;
	}
	
	public static void subscribeToBroker(String topic) {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(brokerhost);
			factory.setPort(brokerPort);
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(topic, false, false, false, null);
			
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				    
					//RabbitMQSpout.nbqueue.add(body);
					String message = new String(body, "UTF-8");
				    System.out.println(" [x] Received '" + message + "'");
				    
				}  
			};
			
			channel.basicConsume(topic, true, consumer);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException t) {
			t.printStackTrace();
		}
	
	}
	
	public static void subscribeToSensorData(String topic) {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(brokerhost);
			factory.setPort(brokerPort);
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(topic, false, false, false, null);
			
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					//com.robert.bosch.schema.validation.SchemaBrokerSpout.nbqueue.add(body);
					String message = new String(body, "UTF-8");
				    System.out.println(" [x] Received '" + message + "'");
				}  
			};
			
			channel.basicConsume(topic, true, consumer);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException t) {
			t.printStackTrace();
		}
	}	
	
	//sample method
	public static void publishToBroker(String topic) {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(brokerhost);
			factory.setPort(brokerPort);
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(topic, false, false, false, null);
			
			String[] msgarr = {"abc","def","ghi","jkl","lmn"};
			for(int i=0;i<msgarr.length;i++) {
				channel.basicPublish("", topic, null, msgarr[i].getBytes());
				System.out.println("[*] Sent: " + msgarr[i]);
			}
			
			channel.close();
			conn.close();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException timeout) {
			timeout.printStackTrace();
		}
		
	}
	
	
	//need to populate it with template_id and schema json string as key and value
	public static void establishCatalogueDBConn() {
		String schema="";
		try {
			URL catURL = new URL("http://10.156.14.5:8001/cat");
			BufferedReader rdr = new BufferedReader(new InputStreamReader(catURL.openStream()));
			String line;
			while((line = rdr.readLine()) != null) {
				schema += line;
				System.out.println(line);
			}
			
			//System.out.println(schema);
			rdr.close();
		} catch(MalformedURLException malurl) {
			malurl.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		//catalogue map gets populated here
		
		
		
	}
}
