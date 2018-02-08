package robertbosch.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeoutException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.protobuf.util.JsonFormat;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
//import com.protoTest.smartcity.Actuated;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Testproto {
	static int ctr=0;
	
	public static void main(String[] args) throws IOException {
//		System.out.println("start...");
//		writeProtodata();
//		System.out.println("stop...");
//		readProtoData();
//		dynamicCompile();
//		readInRemoteMode();
//		
//		ProcessBuilder builder = new ProcessBuilder("/Users/sahiltyagi/Downloads/apache-maven-3.5.2/bin/mvn", "clean", "compile", "assembly:single");
//		builder.directory(new File("/Users/sahiltyagi/Documents/IISc/protoschema"));
//		Process compile = builder.start();
//		try {
//			int complete = compile.waitFor();
//		} catch(InterruptedException in) {
//			in.printStackTrace();
//		}
//		System.out.println("done waiting for compiling");
//		checkprotoJAR();
		
//		System.out.println("test rabbitmq subscriber...");
//		subscriberabbitMQ(1000);
//		System.out.println("done with test proto code...");
		
		String url = "https://raw.githubusercontent.com/mukuntharun/flowsensor/master/protos/sensed.proto";
		System.out.println(RobertBoschUtils.protofiles + url.split("/")[url.split("/").length -1]);
		
	}
	
	private static void subscriberabbitMQ(int datapoint) {
		String subscribefile = "/Users/sahiltyagi/Desktop/subscribe.txt";
		JSONParser parser = new JSONParser();
		RobertBoschUtils rb = new RobertBoschUtils();
		try {
			BufferedWriter subscriber = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(subscribefile)));
			
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(RobertBoschUtils.props.getProperty("host"));
			factory.setPort(Integer.parseInt(RobertBoschUtils.props.getProperty("port")));
			factory.setUsername(RobertBoschUtils.props.getProperty("username"));
			factory.setPassword(RobertBoschUtils.props.getProperty("password"));
			factory.setVirtualHost(RobertBoschUtils.props.getProperty("virtualhost"));
			
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare("sahil", false, false, false, null);
			
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				    
					//RabbitMQSpout.nbqueue.add(body);
					String message = new String(body, "UTF-8");
				    System.out.println(" [x] Received '" + message + "'");
				  
					try {
						Object ob = parser.parse(message);
						JSONObject jsonob = (JSONObject)ob;
						System.out.println(jsonob.get("devEUI"));
						subscriber.write(System.currentTimeMillis() + "," + jsonob.get("devEUI") + "\n");
						ctr++;
						if(ctr == datapoint) {
							subscriber.close();
						}
						
					} catch (ParseException e) {
						e.printStackTrace();
					} 
				}  
			};
			
			channel.basicConsume("sahil", true, consumer);
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException t) {
			t.printStackTrace();
		}
		
	}
	
	private static void checkprotoJAR() {
		try {
			Class cls = Class.forName("com.protoTest.smartcity.Prototest");
			Class[] arr2 = {};
			Method m2 = cls.getDeclaredMethod("start", arr2);
			Object printer = m2.invoke(cls, null);
			System.out.println("end");
			
		} catch(ClassNotFoundException cl) {
			cl.printStackTrace();
		} catch(NoSuchMethodException nomethod) {
			nomethod.printStackTrace();
		} catch(InvocationTargetException invoke) {
			invoke.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
	
	private static void readProtoData() {
		
//		try {
//			Actuated.targetConfigurations confs = Actuated.targetConfigurations.parseFrom(new FileInputStream("/Users/sahiltyagi/Desktop/out1.txt"));
////			Actuated.targetConfigurations confs = Actuated.targetConfigurations.parseFrom(data)
////			System.out.println(confs.getPowerState().getTargetPowerState());
////			System.out.println(confs.getControlPolicy().getControlPolicy());
////			System.out.println(confs.getManualControlParams().getTargetBrightnessLevel());
//			
//			Object ob = Actuated.targetConfigurations.parseFrom(new FileInputStream("/Users/sahiltyagi/Desktop/out1.txt"));
//			String packet=JsonFormat.printer().print((Actuated.targetConfigurations)ob);
//			System.out.println(packet);
//			
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
	}
	
	private static void dynamicCompile() {
		try {
			String urlstr = "https://raw.githubusercontent.com/rbccps-iisc/applications-streetlight/master/proto_stm/rxmsg/actuated.proto";
			URL url = new URL(urlstr);
			BufferedReader rdr = new BufferedReader(new InputStreamReader(url.openStream()));
			String schema="";
			String proto;
			BufferedWriter bfrwrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/actuated3.proto")));
			
			while((proto = rdr.readLine()) != null) {
				schema += proto;
				bfrwrtr.write(proto+"\n");
				if(proto.equals("syntax = \"proto2\";")) {
					bfrwrtr.write("option java_package= \"com.protoTest.smartcity\";");
				}
				
			}
			rdr.close();
			System.out.println(schema);
			bfrwrtr.close();
			
			// one variable to set location of generated .proto files, another variable to specify the package to place generated java class into it
			String[] command = {"/usr/local/bin/protoc", "--proto_path=/Users/sahiltyagi/Desktop", 
							"--java_out=/Users/sahiltyagi/Documents/IISc/protoschema/src/main/java", "actuated5.proto"};
			Process proc = Runtime.getRuntime().exec(command);
			int protogen = proc.waitFor();
			
			ProcessBuilder builder = new ProcessBuilder("/Users/sahiltyagi/Downloads/apache-maven-3.5.2/bin/mvn", "clean", "compile", "assembly:single");
			builder.directory(new File("/Users/sahiltyagi/Documents/IISc/protoschema"));
			Process compile = builder.start();
			int wait = compile.waitFor();
			
			System.out.println("done");
			
		} catch(MalformedURLException urlex) {
			urlex.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(InterruptedException in) {
			in.printStackTrace();
		}
	}
	
	private static void readInRemoteMode() {
		try {
			
			Class cls = Class.forName("com.protoTest.smartcity.Actuated5$targetConfigurations");
//			for(Method method : cls.getMethods()) {
//				System.out.println(method.getName());
//			}
			
			Class[] arr = {InputStream.class};
			Method method = cls.getDeclaredMethod("parseFrom", arr);
			Object packet = method.invoke(cls, new FileInputStream("/Users/sahiltyagi/Desktop/out2.txt"));
			//System.out.println(packet.toString());
			
			Class format = Class.forName("com.google.protobuf.util.JsonFormat");
			Class[] arr2 = {};
			Method m2 = format.getDeclaredMethod("printer", arr2);
			Object printer = m2.invoke(format, null);
//			System.out.println(printer.getClass());			
//			com.google.protobuf.MessageOrBuilder
//			for(Method m : printer.getClass().getMethods()) {
//				System.out.println(m.getName());
//				for(Parameter param : m.getParameters()) {
//					System.out.println(param.getParameterizedType().getTypeName());
//				}
//			}
			
			Class[] arr3 = {Class.forName("com.google.protobuf.MessageOrBuilder")};
			Method m3 = printer.getClass().getDeclaredMethod("print", arr3);
			Object data = m3.invoke(printer, packet);
			System.out.println(data.toString());
			System.out.println("done.");
			
		} catch(ClassNotFoundException c) {
			c.printStackTrace();
		} catch(NoSuchMethodException method) {
			method.printStackTrace();
		} catch(FileNotFoundException f) {
			f.printStackTrace();
		} catch(IllegalAccessException acc) {
			acc.printStackTrace();
		} catch(InvocationTargetException invoke) {
			invoke.printStackTrace();
		}
	}
	
	private static void writeProtodata() {
		
////		Actuated.targetPowerStateParams.Builder powerstate = Actuated.targetPowerStateParams.newBuilder();
////		powerstate.setTargetPowerState(true);
////		
////		Actuated.targetControlPolicy.Builder ctrlpolicy = Actuated.targetControlPolicy.newBuilder();
////		ctrlpolicy.setControlPolicy(Actuated.ctrlPolicy.AUTO_LUX);
////		
////		Actuated.targetManualControlParams.Builder manualparams = Actuated.targetManualControlParams.newBuilder();
////		manualparams.setTargetBrightnessLevel(99);
//		
//		Actuated.targetAutoTimerParams.Builder autotimers = Actuated.targetAutoTimerParams.newBuilder();
//		autotimers.setTargetOnTime(60000);
//		autotimers.setTargetOffTime(120000);
//		
////		Actuated.targetAutoLuxParams.Builder autolux = Actuated.targetAutoLuxParams.newBuilder();
////		autolux.setTargetOnLux(199);
////		autolux.setTargetOffLux(299);
//		
//		Actuated.targetConfigurations.Builder confs = Actuated.targetConfigurations.newBuilder();
////		confs.setPowerState(powerstate);
////		confs.setControlPolicy(ctrlpolicy);
////		confs.setManualControlParams(manualparams);
//		confs.setAutoTimerParams(autotimers);
////		confs.setAutoLuxParams(autolux);
//		
//		Actuated.targetConfigurations finalconf = confs.build();
//		try {
//			
//			finalconf.writeTo(new FileOutputStream("/Users/sahiltyagi/Desktop/out1.txt"));
//			
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
//		
//		System.out.println("done writing proto data to file");
	}
	
}