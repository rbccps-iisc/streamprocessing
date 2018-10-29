package robertbosch.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
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
import com.google.protobuf.util.JsonFormat;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.Filter;
import com.aerospike.client.Value;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.query.IndexType;

import robertbosch.schema.validation.JSONmessagespout;
import robertbosch.schema.validation.Networkserverspout;
import robertbosch.schema.validationservice.BatchGeneratorSpout;



public class RobertBoschUtils implements MqttCallback {
	public static Properties props = new Properties();
	public static Boolean loadingDone=false;
	public static ConcurrentHashMap<String, String> catalogue = new ConcurrentHashMap<String, String>();
	private static List<String> list;
	public static Channel publishchannel;
	public static String pubTopic = "data_out", protofiles = "/home/etl_subsystem/protos/";
	//public static String pubTopic = "valid_data", protofiles = "/Users/sahiltyagi/Desktop/protos/";
	
	static {
		
		props.setProperty("host", "10.156.14.6");
		props.setProperty("port", "5672");
		props.setProperty("username", "rbccps");
		props.setProperty("password", "rbccps@123");
		props.setProperty("exchange", "amq.topic");
		props.setProperty("bindingkey", "*.#");
		props.setProperty("virtualhost", "/");
		props.setProperty("queuename", "database_queue");
		props.setProperty("catalogue", "http://10.156.14.5:8001/cat");
		//props.setProperty("catalogue", "https://smartcity.rbccps.org/api/0.1.0/cat");
		
		//local test config
//		props.setProperty("protocompiler", "/usr/local/bin/protoc");
//		props.setProperty("protopath", "/Users/sahiltyagi/Desktop");
//		props.setProperty("javapath", "/Users/sahiltyagi/Documents/IISc/protoschema/src/main/java");
//		props.setProperty("maven", "/Users/sahiltyagi/Downloads/apache-maven-3.5.2/bin/mvn");
//		props.setProperty("schemarepo", "/Users/sahiltyagi/Documents/IISc/protoschema");
		
		//cluster config
		props.setProperty("protocompiler", "/usr/local/bin/protoc");
		props.setProperty("protopath", "/home/etl_subsystem/protos");
		props.setProperty("javapath", "/home/etl_subsystem/protoschema/src/main/java");
		props.setProperty("maven", "/usr/bin/mvn");
		props.setProperty("schemarepo", "/home/etl_subsystem/protoschema");
		props.setProperty("protoschemajar", "/home/etl_subsystem/protoschema/target/protoschema-1.0-SNAPSHOT-jar-with-dependencies.jar");
		props.setProperty("stormdir", "/home/etl_subsystem/apache-storm-1.0.2");
		
	}
	
	public static boolean validatesensorschema(String schema, String data) {
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
	
	//local function
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
	
	public static void main(String[] args) {
		System.out.println("starting subscribe...");
		getBatch();
	}
	
	public static void subscribeToBrokerData() {
		try {
			String deviceId=null;
			ConnectionFactory factory = new ConnectionFactory();
			
			//factory.setHost("13.58.190.153");
			//factory.setPort(12082);
			
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			factory.setUsername(props.getProperty("username"));
			factory.setPassword(props.getProperty("password"));
			factory.setVirtualHost(props.getProperty("virtualhost"));

			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			//channel.exchangeDeclare(props.getProperty("exchange"), deviceId, true);
			channel.exchangeDeclare(props.getProperty("exchange"), "topic", true);
			
			System.out.println("going to subscribe for device: " + deviceId);
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					if(body != null) {
						JSONmessagespout.jsonqueue.add(body);
						//System.out.println("message:"+ new String(body, "UTF-8"));
					}
				}  
			};
			
			channel.queueDeclare(props.getProperty("queuename"), true, false, false, null);
			channel.queueBind(props.getProperty("queuename"), props.getProperty("exchange"), props.getProperty("bindingkey"));
			channel.basicConsume(props.getProperty("queuename"), true, consumer);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void getBatch() {

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		Statement stmt = new Statement();

		//TODO:confirm namespace and name in CDX deployment
		stmt.setNamespace("test");
		stmt.setSetName("demo1");
		long endEpoc=System.currentTimeMillis();
		//create batch from messages from last two minutes
		long startEpoc=endEpoc-120000;
		stmt.setFilters(Filter.range("timestamp", startEpoc,endEpoc));
		// Execute the query.
		RecordSet recordSet = client.query(null, stmt);
		// Process the record set.
		try {
			while (recordSet != null && recordSet.next()) {
				Key key = recordSet.getKey();
				Record record = recordSet.getRecord();
				org.json.JSONObject jsonObject=new org.json.JSONObject(record.bins);
				String jsonString=jsonObject.toString();
				BatchGeneratorSpout.jsonqueue.add(jsonString.getBytes());
//                System.out.println(jsonString);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally {
			recordSet.close();
		}
	}
	
	
	public static void getScrollBatches() {
		try {
			TransportClient  client = null;
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 5601));//TODO:change this to 5601 from 9300 which is the default elastic search port in CDX deployment
										
			
			//FOR DEBUGGING 
//			String dateString="2017-11-15 14:13:12";
//			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
			
			
		
			Date toDate=new Date(); 
			Calendar cal = Calendar.getInstance();
			cal.setTime(toDate);
			cal.add(Calendar.MINUTE, -2);
			Date fromDate = cal.getTime();
//			System.out.println("FromDate:" + fromDate.toString());
			
			//TODO: confirm the Timestamp field for each message
			QueryBuilder qb =QueryBuilders.rangeQuery("postDate").from(fromDate.toInstant().toEpochMilli()).to(toDate.toInstant().toEpochMilli());
			
			//removed index name from prepareSearch for it to search across all indexes
			SearchResponse scrollResp = client.prepareSearch()
			        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
			        .setScroll(new TimeValue(60000))
			        .setQuery(qb)
			        .setSize(10000).get(); 
			
			do {
			    for (SearchHit h : scrollResp.getHits().getHits()) {
			    	Map m=h.getSource();
					org.json.JSONObject jsonObject=new org.json.JSONObject(m);
					String jsonString=jsonObject.toString();
					BatchGeneratorSpout.jsonqueue.add(jsonString.getBytes());
//					System.out.println("JsonString:" + jsonString);
			    }

			    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
			} while(scrollResp.getHits().getHits().length != 0);
		
		}catch(Exception e) {
			e.printStackTrace();
			
		}
		
	}
	
	public void subscribeToNetworkServer() {
		try {
			
			MqttConnectOptions connection = new MqttConnectOptions();
			connection.setAutomaticReconnect(true);
			connection.setCleanSession(false);
			connection.setConnectionTimeout(30);
			connection.setUserName("loraserver");
			connection.setPassword("loraserver".toCharArray());
			
			MqttClient client = new MqttClient("tcp://gateways.rbccps.org:1883", MqttClient.generateClientId());
			client.setCallback(this);
			client.connect(connection);
			client.subscribe("application/1/node/+/rx", 2);
			//client.subscribe("sahil1", 2);
			
		} catch(MqttException e) {
			e.printStackTrace();
		}
		
	}
	
	//local function
	public static void publishToBroker(String topic) {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			factory.setUsername(props.getProperty("username"));
			factory.setPassword(props.getProperty("password"));
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(topic, false, false, false, null);
			
			String[] msgarr = {"abc","def","ghi","jkl","lmn"};
			for(int i=0;i<msgarr.length;i++) {
				channel.basicPublish("", topic, null, msgarr[i].getBytes());
				System.out.println("###### published a message to broker");
			}
			
			channel.close();
			conn.close();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException timeout) {
			timeout.printStackTrace();
		}
		
	}
	
	public void getPublishChannel() {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(props.getProperty("host"));
			factory.setPort(Integer.parseInt(props.getProperty("port")));
			factory.setUsername(props.getProperty("username"));
			factory.setPassword(props.getProperty("password"));
			
			Connection conn = factory.newConnection();
			publishchannel = conn.createChannel();
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException timeout) {
			timeout.printStackTrace();
		}
	}
	
	
	//need to populate it with template_id and schema json string as key and value
	public void queryCatalogueServer() {
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
				
				String devId = itemobj.get("id").toString();
				System.out.println(devId);
				
				//using wildcard operator to subscribe to all topics
				if(!Networkserverspout.deviceprotoschema.containsKey(devId)) {
					
					String schema = itemobj.get("data_schema").toString();
					catalogue.put(devId, schema);
					
					if(itemobj.keySet().contains("serialization_from_device")) {
						
						Object ob2 = itemobj.get("serialization_from_device");
						JSONObject jsonob2 = (JSONObject)ob2;
						//System.out.println(jsonob2.get("schema_ref").toString());
						ob2 = jsonob2.get("schema_ref");
						jsonob2 = (JSONObject)ob2;
						
						if(jsonob2.keySet().contains("mainMessageName")) {
							
							System.out.println(jsonob2.get("link").toString() + "  " + jsonob2.get("mainMessageName").toString());
							Networkserverspout.deviceprotoschema.put(devId, jsonob2.get("link").toString() + "___" + jsonob2.get("mainMessageName").toString());
						}	
					}	
				}
			}
			
			System.out.println("fully read the catalogue server...");
		} catch(ParseException pex) {
			pex.printStackTrace();
		}
	}
	
//	private static void validatesensordata() {
//		list = new ArrayList<String>();
//		//subscribeToSensorData();
//		while(true) {
//			if(list.size() > 0) {
//				int index=0;
//				while(index < list.size()) {
//					//RBCCPS_EM_1111
//					String json = list.get(index);
//					JSONParser parse = new JSONParser();
//					try {
//						Object obj = parse.parse(json);
//						JSONObject jsonob = (JSONObject)obj;
//						if(jsonob.containsKey("key")) {
//							String devId = jsonob.get("key").toString();
//							//String data = json.split(",")[1].replaceAll("]", "");
//							String data = jsonob.get("data").toString();
//							System.out.println("data is: " + data);
//							
//							boolean status = validatesensorschema(catalogue.get(devId), data);
//							if(status) {
//								System.out.println("Voila! It's a match for: " + list.get(index));
//							} else {
//								System.out.println("not a match for: " + list.get(index));
//							}
//							
//							list.remove(index);
//							//index++;
//						}
////						else {
////							System.out.println("########## key not present!");
////						}
//						
//					} catch(ParseException p) {
//						p.printStackTrace();
//					}
//				}
//			}
//		}
//	}
	
	public static ConcurrentLinkedQueue<byte[]> arrtest = new ConcurrentLinkedQueue<byte[]>();

	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		System.out.println("lost connection to LoRA server");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		System.out.println("delivery complete....");
	}

	@Override
	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		// TODO Auto-generated method stub
		Networkserverspout.loraserverqueue.add(arg1.getPayload());
		//arrtest.add(arg1.getPayload());
	}

	public static synchronized void loadCatalogue() {
		
		if(!loadingDone) {
			// Create a trust manager that does not validate certificate chains
			TrustManager[] trustAllCerts = new TrustManager[]{
			    (TrustManager) new X509TrustManager() {
			        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			            return null;
			        }
			        public void checkClientTrusted(
			            java.security.cert.X509Certificate[] certs, String authType) {
			        }
			        public void checkServerTrusted(
			            java.security.cert.X509Certificate[] certs, String authType) {
			        }
			    }
			};

			// Install the all-trusting trust manager
			try {
			    SSLContext sc = SSLContext.getInstance("SSL");
			    sc.init(null, trustAllCerts, new java.security.SecureRandom());
			    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			} catch (Exception e) {

			}
			
		String newDummySchema="{\n" + 
				"  \"definitions\": {},\n" + 
				"  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" + 
				"  \"$id\": \"http://example.com/root.json\",\n" + 
				"  \"type\": \"object\",\n" + 
				"  \"title\": \"The Root Schema\",\n" + 
				"  \"required\": [\n" + 
				"    \"key\",\n" + 
				"    \"postDate\",\n" + 
				"    \"content\",\n" + 
				"    \"age\"\n" + 
				"  ],\n" + 
				"  \"properties\": {\n" + 
				"    \"key\": {\n" + 
				"      \"$id\": \"#/properties/key\",\n" + 
				"      \"type\": \"string\",\n" + 
				"      \"title\": \"The Key Schema\",\n" + 
				"      \"default\": \"\",\n" + 
				"      \"examples\": [\n" + 
				"        \"tweetSensor1\"\n" + 
				"      ],\n" + 
				"      \"pattern\": \"^(.*)$\"\n" + 
				"    },\n" + 
				"    \"postDate\": {\n" + 
				"      \"$id\": \"#/properties/postDate\",\n" + 
				"      \"type\": \"string\",\n" + 
				"      \"title\": \"The Postdate Schema\",\n" + 
				"      \"default\": \"\",\n" + 
				"      \"examples\": [\n" + 
				"        \"2009-11-15T14:12:12\"\n" + 
				"      ],\n" + 
				"      \"pattern\": \"^(.*)$\"\n" + 
				"    },\n" + 
				"    \"content\": {\n" + 
				"      \"$id\": \"#/properties/content\",\n" + 
				"      \"type\": \"string\",\n" + 
				"      \"title\": \"The Content Schema\",\n" + 
				"      \"default\": \"\",\n" + 
				"      \"examples\": [\n" + 
				"        \"Hello World!\"\n" + 
				"      ],\n" + 
				"      \"pattern\": \"^(.*)$\"\n" + 
				"    },\n" + 
				"    \"age\": {\n" + 
				"      \"$id\": \"#/properties/age\",\n" + 
				"      \"type\": \"string\",\n" + 
				"      \"title\": \"The Age Schema\",\n" + 
				"      \"default\": \"\",\n" + 
				"      \"examples\": [\n" + 
				"        \"23\"\n" + 
				"      ],\n" + 
				"      \"pattern\": \"^(.*)$\"\n" + 
				"    }\n" + 
				"  }\n" + 
				"}";
		
		//Adding a dummy schema for testing
		RobertBoschUtils.catalogue.put("tweetSensor1", newDummySchema);
		String Response = "";
		//TODO: read catalog url from a config file
		String catalogueUrl = "https://localhost:8443/api/1.0.0/cat";
		//READING the whole catalog
		try {
		URL url = new URL(catalogueUrl);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
		int status = con.getResponseCode();
		BufferedReader in = new BufferedReader(
				  new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer content = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
				    content.append(inputLine);
				}
				in.close();
				con.disconnect();
				Response=content.toString();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		//Parse the content response to populate the in memory catalogue..TODO:Add locks to share in memory catalogue
		final org.json.JSONObject obj = new org.json.JSONObject(Response);
		final org.json.JSONArray catalogueEntries = obj.getJSONArray("items");
		final int n = catalogueEntries.length();
	    for (int i = 0; i < n; ++i) {
	      final org.json.JSONObject catalogueEntry = catalogueEntries.getJSONObject(i);
//	      System.out.println(person.getInt("id"));
//	      System.out.println(catalogueEntry.toString());
	      RobertBoschUtils.catalogue.put(catalogueEntry.getString("id"), catalogueEntry.toString());
	    }
		//setting flag so that other threads don't load the whole catalog again
	    loadingDone=true;
	    
	    
		}//if ended
	}
	
	//Check if this code works...
	public static String queryCatalog(String deviceId) {
		// Create a trust manager that does not validate certificate chains
				TrustManager[] trustAllCerts = new TrustManager[]{
				    (TrustManager) new X509TrustManager() {
				        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				            return null;
				        }
				        public void checkClientTrusted(
				            java.security.cert.X509Certificate[] certs, String authType) {
				        }
				        public void checkServerTrusted(
				            java.security.cert.X509Certificate[] certs, String authType) {
				        }
				    }
				};

				// Install the all-trusting trust manager
				try {
				    SSLContext sc = SSLContext.getInstance("SSL");
				    sc.init(null, trustAllCerts, new java.security.SecureRandom());
				    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
				} catch (Exception e) {

				}
				
		String jsonSchema="";
		String Response = "";
		String catalogueUrl = "https://localhost:8443/api/1.0.0/cat?id="+ deviceId;
		//READING the whole catalog
		try {
		URL url = new URL(catalogueUrl);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
		int status = con.getResponseCode();
		BufferedReader in = new BufferedReader(
				  new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer content = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
				    content.append(inputLine);
				}
				in.close();
				con.disconnect();
				Response=content.toString();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		//parse the response to extract the catalog entry..
		final org.json.JSONObject obj = new org.json.JSONObject(Response);
		final org.json.JSONArray catalogueEntries = obj.getJSONArray("items");
		final int n = catalogueEntries.length();
	    for (int i = 0; i < n; ++i) {
	      final org.json.JSONObject catalogueEntry = catalogueEntries.getJSONObject(i);
	      jsonSchema = catalogueEntry.toString();
	      break;
	    }		
				
		return jsonSchema;		
	}
	
}
