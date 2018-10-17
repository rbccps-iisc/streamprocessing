package robertbosch.schema.validationservice;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import robertbosch.utils.RobertBoschUtils;

public class SchemaValidatorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String deviceId, jsondata;
	boolean status;
	HashMap<String,Long> invalidMessageCounter;

	@Override
	public void execute(Tuple arg0) {
		
		RobertBoschUtils.loadCatalogue();
		// TODO Auto-generated method stub
		deviceId = arg0.getStringByField("deviceid");
		jsondata = arg0.getStringByField("jsondata");
		
		//get appropriate schema from hashmap and call method validatesensorschema to get boolean result. If true, data is valid else discard it
		if(RobertBoschUtils.catalogue.containsKey(deviceId)) {
			status = RobertBoschUtils.validatesensorschema(RobertBoschUtils.catalogue.get(deviceId), jsondata);
		} else {
			//schema needs to be populated from the catalog
			String jsonSchema=RobertBoschUtils.queryCatalog(deviceId);
			RobertBoschUtils.catalogue.put(deviceId, jsonSchema);
		
			status = RobertBoschUtils.validatesensorschema(jsonSchema, jsondata);
		}
		
		//push the data to broker from here...
		if(status) {
			System.out.println("the following data was successfully validated:" + jsondata);
			//TODO:mark as valid json and push it to elastic search
		}
		else
		{
//			System.out.println("INVALID json:" + jsondata);
			//TODO:push this json marked as invalidated back to elastic search
			Long count=this.invalidMessageCounter.get(deviceId);
			if(count==null) {
				count=0L;
			}else
			{
				count++;
			}
			invalidMessageCounter.put(deviceId, count);
			
		}
		
		
		for(Entry<String, Long> entry: invalidMessageCounter.entrySet()) {
			Values vals = new Values(entry.getKey(),entry.getValue());
			
			outputCollector.emit(vals);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		this.invalidMessageCounter=new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "count"));
	}

	
	
	
	
	
}
