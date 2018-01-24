package robertbosch.schema.validation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import robertbosch.utils.RobertBoschUtils;

public class SchemaVerifyBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static ConcurrentLinkedQueue<String> protos=null;

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String sensordata = tuple.getStringByField("sensordata");
		boolean status=false;
		
		//json parser here for incoming data packet read from non-blocking queue
		//get appropriate schema from hashmap and call method validateSchema to get boolean result. If true, data is valid else discard it
		//fetch type of data (energy meter, street light etc.) from json sensor data, and this will be key of the hash
		
		JSONParser parser = new JSONParser();
		String devId=null, data=null;
		try {
			
			Object obj = parser.parse(sensordata);
			JSONObject jsonobj = (JSONObject)obj;
			if(jsonobj.containsKey("key")) {
				devId = jsonobj.get("key").toString();
				data = sensordata.split(",")[1].replaceAll("]", "").trim();
				
				if(!RobertBoschUtils.catalogue.containsKey(devId)) {
					//establish database conn and fill the hashmap again
					RobertBoschUtils.establishCatalogueDBConn();
					status = RobertBoschUtils.validateSchema(RobertBoschUtils.catalogue.get(devId), data);
				} else {
					status = RobertBoschUtils.validateSchema(RobertBoschUtils.catalogue.get(devId), data);
				}
				
				System.out.println("############################################ value of status: " + status);
				if(status) {
					//publish to rabbitmq topic
					try {
						RobertBoschUtils.publishchannel.queueDeclare(RobertBoschUtils.pubTopic, false, false, false, null);
						RobertBoschUtils.publishchannel.basicPublish("", RobertBoschUtils.pubTopic, null, sensordata.getBytes());
					} catch(IOException e) {
						e.printStackTrace();
					}	
				}
				
			} else {
				System.out.println("$$$$$$ data not in desired schema: " + sensordata);
			}
			
		} catch(ParseException p) {
			p.printStackTrace();
		}	
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		protos = new ConcurrentLinkedQueue();
		RobertBoschUtils utils = new RobertBoschUtils();
		RobertBoschUtils.getPublishChannel();
	}

	public void declareOutputFields(OutputFieldsDeclarer fields) {
		// TODO Auto-generated method stub
		fields.declare(new Fields("sensordata"));
	}
}
