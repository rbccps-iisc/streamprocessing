package robertbosch.schema.validation;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import robertbosch.utils.RobertBoschUtils;

public class JSONmessagespout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> jsonqueue;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(jsonqueue !=null && !jsonqueue.isEmpty()) {
			
			byte[] buffer = jsonqueue.poll();
			try {
				
				String data = new String(buffer, "UTF-8");
				Values vals = new Values("json", "*DEVICE ID*", data);
				spoutcollector.emit(vals);
				vals.clear();
			} catch(UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		spoutcollector = arg2;
		jsonqueue = new ConcurrentLinkedQueue<byte[]>();
		//subscribe to rabbitMQ broker
		RobertBoschUtils.subscribeToBrokerData();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("type", "devEUI", "data"));
	}

}
