package robertbosch.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

import robertbosch.schema.validation.SchemaBrokerSpout;

public class RobertBoschUtils {
	public static Properties props = new Properties();
	public static ConcurrentHashMap<String, String> catalogue = new ConcurrentHashMap<String, String>();
	
	static {
		
		props.setProperty("host", "10.156.14.9");
		props.setProperty("port", "5672");
		props.setProperty("username", "rbccps");
		props.setProperty("password", "rbccps@123");
		props.setProperty("exchange", "amq.topic");
		props.setProperty("bindingkey", "*.#");
		props.setProperty("virtualhost", "/");
		props.setProperty("queuename", "database_queue");
		props.setProperty("catalogue", "http://10.156.14.5:8001/cat");
		
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
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			
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
	
	public static void subscribeToSensorData() {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			factory.setUsername(props.getProperty("username"));
			factory.setPassword(props.getProperty("password"));
			factory.setVirtualHost(props.getProperty("virtualhost"));
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.exchangeDeclare(props.getProperty("exchange"), "topic", true);
			
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					SchemaBrokerSpout.nbqueue.add(body);
					String message = new String(body, "UTF-8");
				    System.out.println(" [x] Received '" + message + "'");
				}  
			};
			
			channel.queueDeclare(props.getProperty("queuename"), true, false, false, null);
			channel.queueBind(props.getProperty("queuename"), props.getProperty("exchange"), props.getProperty("bindingkey"));
			channel.basicConsume(props.getProperty("queuename"), true, consumer);
			
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
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			
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
		String catstring="";
		try {
			URL catURL = new URL(props.getProperty("catalogue"));
			BufferedReader rdr = new BufferedReader(new InputStreamReader(catURL.openStream()));
			String line;
			while((line = rdr.readLine()) != null) {
				catstring += line;
			}
			
			rdr.close();
		} catch(MalformedURLException malurl) {
			malurl.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		//catalogue map gets populated here
		JSONParser parse = new JSONParser();
		try {
			Object obj = parse.parse(catstring);
			JSONObject jsonobj = (JSONObject)obj;
			
			JSONArray items = (JSONArray)jsonobj.get("items");
			Iterator<JSONObject> itr = items.iterator();
			while(itr.hasNext()) {
				JSONObject itemobj = itr.next();
				String href = itemobj.get("href").toString();
				JSONArray itemMetaArr = (JSONArray)itemobj.get("item-metadata");
				Iterator<JSONObject> it = itemMetaArr.iterator();
				while(it.hasNext()) {
					JSONObject metaobj = it.next();
					String schema = metaobj.get("data_schema").toString();
					catalogue.put(href, schema);
				}
			}
			
		} catch(ParseException pex) {
			pex.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		//establishCatalogueDBConn();
		subscribeToSensorData();
	}
	
}
