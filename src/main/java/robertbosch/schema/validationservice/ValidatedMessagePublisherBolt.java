package robertbosch.schema.validationservice;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import robertbosch.utils.RobertBoschUtils;
import java.util.Map;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.Vertx;
//import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.Host;
//import com.aerospike.client.policy.ClientPolicy;
//import com.aerospike.client.Key;
//import com.aerospike.client.Bin;
//import com.aerospike.client.Record;
//import com.aerospike.client.query.Statement;
//import com.aerospike.client.query.Filter;
//import com.aerospike.client.Value;
//import com.aerospike.client.query.RecordSet;
//import com.aerospike.client.task.IndexTask;
//import com.aerospike.client.query.IndexType;



public class ValidatedMessagePublisherBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String deviceId,jsondata,valid;
	RabbitMQOptions config = null;
	RabbitMQClient client = null;
	Vertx vertx = null;
	// long count;
	// boolean status;
//	HashMap<String,Long> invalidMessageCounter;

	@Override
	public void execute(Tuple arg0) {
		deviceId = arg0.getStringByField("deviceid");
		jsondata = arg0.getStringByField("jsondata");
		valid=arg0.getStringByField("valid");


// full amqp uri TODO: set amqp uri

config.setUri("amqp://xvjvsrrc:VbuL1atClKt7zVNQha0bnnScbNvGiqgb@moose.rmq.cloudamqp.com/xvjvsrrc");


if(valid.equals("true")) {

//publish message to broker...TODO:change the queue name
	JsonObject message = new JsonObject().put("body", jsondata);
	client.basicPublish("", "validated.queue", message, pubResult -> {
		if (!pubResult.succeeded()) {
			pubResult.cause().printStackTrace();
		}
	});

}
else{


}




		// AerospikeClient client = new AerospikeClient("localhost", 3000);
		// org.json.JSONObject jsonObject=new org.json.JSONObject(jsondata);
		// // jsonObject.put("valid",valid);
		// // jsonObject.put("checked",checked);
		// String jsonString=jsonObject.toString();
		// Key validationKey = new Key("test", "demo1", jsonObject.get("id").toString() + "-" + jsonObject.get("timestamp") +"-Validated");
		// Bin bodyBin=new Bin("body",jsonString);
		// client.put(null,validationKey,bodyBin);
		//
		// client.close();
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		config = new RabbitMQOptions();
		vertx = Vertx.vertx();
		client = RabbitMQClient.create(vertx, config);

//		this.invalidMessageCounter=new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "jsondata","valid"));
	}

}
