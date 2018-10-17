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

public class CounterBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String deviceId;
	long count;
	boolean status;
	HashMap<String,Long> invalidMessageCounter;

	@Override
	public void execute(Tuple arg0) {
		
		RobertBoschUtils.loadCatalogue();
		// TODO Auto-generated method stub
		deviceId = arg0.getStringByField("deviceid");
		count = arg0.getLongByField("count");
		invalidMessageCounter.get(deviceId);
		Long totalCount=this.invalidMessageCounter.get(deviceId);
		if(totalCount==null) {
			totalCount=0L;
		}
		invalidMessageCounter.put(deviceId, totalCount+count);

		if((totalCount+count)>10) {
			System.out.println("Found Anomalous Sensor" + deviceId);
			invalidMessageCounter.put(deviceId,0L);
			
			//TODO:Do some actions
			
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
