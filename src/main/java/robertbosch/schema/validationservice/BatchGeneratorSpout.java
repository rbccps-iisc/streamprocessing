package robertbosch.schema.validationservice;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

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

public class BatchGeneratorSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> jsonqueue;
	String deviceid, data;
	JSONParser parser ;
	JSONObject jsonob;
	Object obj;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		RobertBoschUtils.getBatch();
		long start=System.currentTimeMillis();
		//logic to split the batch into messages and forward it to validator
		while(jsonqueue !=null && !jsonqueue.isEmpty()) {
			byte[] buffer = jsonqueue.poll();
			try {
				String data = new String(buffer, "UTF-8");
				obj = parser.parse(data);
				jsonob = (JSONObject)obj;
				
				Values vals = new Values(jsonob.get("id"), jsonob.toString());//check if only data needs to be verified
				spoutcollector.emit(vals);
			} catch(UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch(ParseException p) {
				p.printStackTrace();
			}
		}
		
		
		//calculating Total time and converting it to seconds
		long timeToProcess=(System.currentTimeMillis()-start)/1000;
		
		
		//TODO: Logic for Multithreaded spout
		try {
			Thread.sleep(120000-timeToProcess);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		spoutcollector = arg2;
		jsonqueue = new ConcurrentLinkedQueue<byte[]>();
		parser = new JSONParser();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "jsondata"));
	}

}
