package robertbosch.schema.validation;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import robertbosch.utils.RobertBoschUtils;

public class SchemaVerifyBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String sensordata = tuple.getStringByField("sensordata");
		boolean status=false;
		
		//json parser here
		
		//get appropriate schema from hashmap and call method validateSchema to get boolean result. If true, data is valid else discard it
		//fetch type of data (energy meter, street light etc.) from json 'sensordata', and this will be key of the map
		
		if(!RobertBoschUtils.catalogue.containsKey("*from sensor data*")) {
			//establish database conn and fill the hashmap again
			RobertBoschUtils.establishCatalogueDBConn();
			status = RobertBoschUtils.validateSchema(RobertBoschUtils.catalogue.get("from sensor data"), sensordata);
		} else {
			status = RobertBoschUtils.validateSchema(RobertBoschUtils.catalogue.get("from sensor data"), sensordata);
		}
		
		if(status) {
			collector.emit(new Values(sensordata));
		}
		
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer fields) {
		// TODO Auto-generated method stub
		fields.declare(new Fields("sensordata"));
	}
}