package robertbosch.schema.validation;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import robertbosch.utils.RobertBoschUtils;

public class SPOUTvalidator extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<ConcurrentLinkedQueue<byte[]>> brokerqueue;
	public ConcurrentLinkedQueue<byte[]> deviceanddata;
	public static ConcurrentHashMap<String, String> deviceprotoschema;
	Values values;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(deviceanddata.size() >0) {
			deviceanddata = brokerqueue.poll();
			if(deviceanddata != null && deviceanddata.size() ==2) {
				try {
					//field 1: device id and field 2: proto data
					values = new Values(new String(deviceanddata.poll(), "UTF-8"), deviceanddata.poll());
					spoutcollector.emit(values);
					
				} catch(UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}	
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.spoutcollector = arg2;
		brokerqueue = new ConcurrentLinkedQueue<ConcurrentLinkedQueue<byte[]>>();
		deviceprotoschema = new ConcurrentHashMap<String, String>();
		//subscribe to all device topics here by collecting all device ids from cat
		//do a subscribe while querying cat only when a new device comes in...checking from queue deviceIds
		RobertBoschUtils.queryCatalogurServer();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "protodata"));
	}

}
