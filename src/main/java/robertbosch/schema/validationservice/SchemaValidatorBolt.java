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
	//HashMap<String,Long> invalidMessageCounter;

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

		String checked="true";
		String valid="false";
		//push the data to broker from here...
		if(status) {
			System.out.println("the following data was successfully validated:" + jsondata);
			valid="true";
		}
		else
		{
			System.out.println("INVALID json:" + jsondata);

		}



		//send count to counter bolt
			Values vals = new Values(deviceId,1);

			outputCollector.emit("counterStream",vals);

			//send to ValidatedMessagePublisherBolt

			vals = new Values(deviceId,jsondata,valid);

			outputCollector.emit("publishValidatedStream",vals);

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		//this.invalidMessageCounter=new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declareStream("publishValidatedStream", new Fields("deviceid", "jsondata","valid"));
    arg0.declareStream("counterStream", new Fields("deviceid", "count"));
		//arg0.declare(new Fields("deviceid", "count"));
	}


}
