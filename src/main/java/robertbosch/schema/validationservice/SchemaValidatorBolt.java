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
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.Filter;
import com.aerospike.client.Value;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.query.IndexType;


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

		String checked="true";
		String valid="false";
		//push the data to broker from here...
		if(status) {
			System.out.println("the following data was successfully validated:" + jsondata);
			//TODO:mark as valid json and push it to backend
			valid="true";
		}
		else
		{
			System.out.println("INVALID json:" + jsondata);
			//TODO:push this json marked as invalidated back to backend
//			Long count=this.invalidMessageCounter.get(deviceId);
//			if(count==null) {
//				count=0L;
//			}else
//			{
//				count++;
//			}
//			invalidMessageCounter.put(deviceId, count);
			
		}

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		org.json.JSONObject jsonObject=new org.json.JSONObject(jsondata);
		jsonObject.put("valid",valid);
		jsonObject.put("checked",checked);
		String jsonString=jsonObject.toString();
		Key validationKey = new Key("test", "demo1", jsonObject.get("id").toString() + "-" + jsonObject.get("timestamp") +"-Validated");
		Bin bodyBin=new Bin("body",jsonString);
		client.put(null,validationKey,bodyBin);

		client.close();
		
//		for(Entry<String, Long> entry: invalidMessageCounter.entrySet()) {
			Values vals = new Values(deviceId,1);
			
			outputCollector.emit(vals);
//		}
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
