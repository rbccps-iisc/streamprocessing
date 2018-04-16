package robertbosch.utils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeoutException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class KongSubAPI {
	static int ctr=0;
	static JSONParser parser = new JSONParser();
	static Object obj=null;
	static JSONObject jsonob=null;
	
	public static void main(String[] args) {
		System.out.println("going to start...");
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/subscribe10000.txt")));
			int itr=10;
			
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("52.14.204.82");
			factory.setPort(12082);
			factory.setUsername("rbccps");
			factory.setPassword("rbccps@123");
			factory.setVirtualHost("/");

			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			//channel.exchangeDeclare(props.getProperty("exchange"), deviceId, true);
			channel.exchangeDeclare("streetLight_1A_212.protected", "topic", true);
			
			Consumer consumer = new DefaultConsumer(channel) {
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					if(body != null) {
						String msg = new String(body, "UTF-8");
						//System.out.println(msg);
						ctr++;
						try {
							obj= parser.parse(msg);
							jsonob = (JSONObject)obj;
							
							writer.write(System.currentTimeMillis() + "," + jsonob.get("msgid") + "\n");
							if(itr==ctr) {
								writer.close();
								System.out.println("done with the consuming....");
							}
							
						} catch(ParseException p) {
							p.printStackTrace();
						}
						
					}
				}  
			};
			
			channel.queueDeclare("database_queue", true, false, false, null);
			channel.queueBind("database_queue", "streetLight_1A_212.protected", "*.#");
			channel.basicConsume("database_queue", true, consumer);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(TimeoutException e) {
			e.printStackTrace();
		}
		
		System.out.println("start consuming...");
	}
}
