package robertbosch.schema.validationservice;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


import robertbosch.utils.RobertBoschUtils;

public class SchemaValidatorBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String deviceId, jsondata;
	boolean status;
	public static LoadingCache<String, String> lruCache;
	public static CacheLoader<String, String> loader;
	public static boolean cacheInitDone=false;


	@Override
	public void execute(Tuple arg0) {

//		RobertBoschUtils.loadCatalogue();
		// TODO Auto-generated method stub
		deviceId = arg0.getStringByField("deviceid");
		jsondata = arg0.getStringByField("jsondata");
		String schema=null;
		try {
			schema=lruCache.get(deviceId);
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		//get appropriate schema from cache and call method validatesensorschema to get boolean result. If true, data is valid else discard it
		if(schema!=null) {
			status = RobertBoschUtils.validatesensorschema(schema, jsondata);
		} else {
			//schema needs to be populated from the catalog
			String jsonSchema=RobertBoschUtils.queryCatalog(deviceId);
			lruCache.put(deviceId, jsonSchema);

			status = RobertBoschUtils.validatesensorschema(jsonSchema, jsondata);
		}

		String checked="true";
		boolean valid=false;

		if(status) {
			System.out.println("the following data was successfully validated:" + jsondata);
			valid=true;
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
		initCache();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declareStream("publishValidatedStream", new Fields("deviceid", "jsondata","valid"));
    	arg0.declareStream("counterStream", new Fields("deviceid", "count"));
		//arg0.declare(new Fields("deviceid", "count"));
	}


	public static synchronized void initCache(){

			if(!cacheInitDone) {
				loader = new CacheLoader<String, String>() {
					@Override
					public String load(String key) {
						return key;
					}
				};


				lruCache = CacheBuilder.newBuilder().build(loader);
				cacheInitDone = true;
			}

	}

}
