package robertbosch.schema.validation;

//import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import robertbosch.utils.RobertBoschUtils;

public class SchemaBrokerSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> nbqueue;
	Values values;

	public void nextTuple() {
		// TODO Auto-generated method stub
		
		if(nbqueue.size() > 0) {
			byte[] data = nbqueue.poll();
			if(data != null) {
//				String sensordata;
//				try {
//					sensordata = new String(data, "UTF-8");
//					values = new Values(sensordata);
//					spoutcollector.emit(values);
//				} catch (UnsupportedEncodingException e) {
//					e.printStackTrace();
//				}
				
				values = new Values(data);
				spoutcollector.emit(values);
				
			}
		}
		
	}

	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.spoutcollector = collector;
		nbqueue = new ConcurrentLinkedQueue<byte[]>();
		RobertBoschUtils.subscribeToSensorData();
		
		//establish database connection to device catalogue...will return a hashmap for datatype key and schema value (streetlight, streetlight schema etc)
		RobertBoschUtils.queryCatalogurServer();
		
	}

	public void declareOutputFields(OutputFieldsDeclarer fields) {
		// TODO Auto-generated method stub
		fields.declare(new Fields("sensordata"));
	}
}
