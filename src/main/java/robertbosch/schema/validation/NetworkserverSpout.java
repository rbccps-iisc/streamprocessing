package robertbosch.schema.validation;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

public class NetworkserverSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> loraserverqueue;
	public static ConcurrentLinkedQueue<String> protoURLs;
	public static ConcurrentHashMap<String, String> deviceprotoschema;
	Values values;
	byte[] lorabinarydata, protobinary;
	String loradata, protodata, deviceid;
	JSONParser parser;
	Object obj;
	JSONObject jsonob;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			if(loraserverqueue != null && loraserverqueue.size() >0) {
				
				lorabinarydata = loraserverqueue.poll();
				loradata = new String(lorabinarydata, StandardCharsets.UTF_8);
				obj = parser.parse(loradata);
				jsonob = (JSONObject)obj;
				deviceid = jsonob.get("devEUI").toString();
				protodata = jsonob.get("data").toString();
				protobinary = Base64.getDecoder().decode(protobinary);
				values = new Values(deviceid, protobinary);
				spoutcollector.emit(values);
				
			}
		} catch(ParseException p) {
			p.printStackTrace();
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.spoutcollector = arg2;
		parser = new JSONParser();
		loraserverqueue = new ConcurrentLinkedQueue<byte[]>();
		deviceprotoschema = new ConcurrentHashMap<String, String>();
		protoURLs = new ConcurrentLinkedQueue<String>();
		RobertBoschUtils rbutils = new RobertBoschUtils();
		rbutils.queryCatalogurServer();
		rbutils.subscribeToNetworkServer();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "protodata"));
	}

}
