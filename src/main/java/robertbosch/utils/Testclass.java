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

public class Testclass {
	static int ctr=0;
	
	public static void main(String[] args) {
		System.out.println("test rabbitmq subscriber...");
		subscriberabbitMQ(Integer.parseInt(args[0]));
		System.out.println("done with test proto code...");
		
	}
	
	private static void subscriberabbitMQ(final int datapoint) {
		//String subscribefile = "/Users/sahiltyagi/Desktop/subscribe.txt";
		String subscribefile = "/home/etl_subsystem/subscribe.txt";
		RobertBoschUtils rb = new RobertBoschUtils();
		try {
			final BufferedWriter subscriber = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(subscribefile)));
			
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
				    JSONParser parser = new JSONParser();
				  
					try {
						Object ob = parser.parse(message);
						JSONObject jsonob = (JSONObject)ob;
						System.out.println(jsonob.get("devEUI"));
						subscriber.write(System.currentTimeMillis() + "," + jsonob.get("devEUI") + "\n");
						ctr++;
						if(ctr == datapoint) {
							subscriber.close();
							System.out.println("completed index....");
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
}