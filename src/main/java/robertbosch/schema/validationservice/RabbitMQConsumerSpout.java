package robertbosch.schema.validationservice;

import com.aerospike.client.Log;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import robertbosch.utils.RobertBoschUtils;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.Vertx;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RabbitMQConsumerSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> jsonqueue;
	String deviceid, data;
	JSONParser parser ;
	JSONObject jsonob;
	Object obj;
	RabbitMQOptions broker_config = null;
	RabbitMQClient client = null;
	Vertx vertx = null;
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

		long start=System.currentTimeMillis();
		//logic to split the batch into messages and forward it to validator

			//TODO:use basicConsume



				while(true) {
					client.basicGet("rawQueue", true, getResult -> {
						if (getResult.succeeded()) {
							try {
								JsonObject msg = getResult.result();
								String data = msg.getString("body");
								obj = parser.parse(data);
								jsonob = (JSONObject) obj;
								Values vals = new Values(jsonob.get("id"), jsonob.toString());//check if only data needs to be verified
								spoutcollector.emit(vals);
							} catch (ParseException p) {
								p.printStackTrace();
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else {
							getResult.cause().printStackTrace();
						}
					});
				}





		}


	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		spoutcollector = arg2;
		jsonqueue = new ConcurrentLinkedQueue<byte[]>();
		parser = new JSONParser();
		vertx = Vertx.vertx();

		broker_config = new RabbitMQOptions();
		broker_config.setHost("localhost");
//        broker_config.setPort(broker_port);
//        broker_config.setVirtualHost(broker_vhost);
//        broker_config.setUser(username);
//        broker_config.setPassword(password);
		broker_config.setConnectionTimeout(6000);
		broker_config.setRequestedHeartbeat(60);
		broker_config.setHandshakeTimeout(6000);
		broker_config.setRequestedChannelMax(5);
		broker_config.setNetworkRecoveryInterval(500);

		client = RabbitMQClient.create(vertx, broker_config);
		client.start(start_handler -> {
			if (start_handler.succeeded()) {

				Log.info("vertx RabbitMQ client started successfully!");
			}
		});

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "jsondata"));
	}

}
