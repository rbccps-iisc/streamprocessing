package robertbosch.schema.validation;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import robertbosch.utils.RobertBoschUtils;

public class SchemaValidatorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String deviceId, jsondata;
	boolean status;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		deviceId = arg0.getStringByField("deviceid");
		jsondata = arg0.getStringByField("jsondata");
		
		//get appropriate schema from hashmap and call method validatesensorschema to get boolean result. If true, data is valid else discard it
		if(RobertBoschUtils.catalogue.containsKey(deviceId)) {
			status = RobertBoschUtils.validatesensorschema(RobertBoschUtils.catalogue.get(deviceId), jsondata);
		} else {
			status= false;
		}
		
		//push the data to broker from here...
		if(status) {
			System.out.println("the following data was successfully validated:" + jsondata);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}

}
