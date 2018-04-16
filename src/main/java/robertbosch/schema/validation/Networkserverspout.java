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

public class Networkserverspout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector spoutcollector;
	public static ConcurrentLinkedQueue<byte[]> loraserverqueue = new ConcurrentLinkedQueue<byte[]>();
	public static ConcurrentHashMap<String, String> deviceprotoschema = new ConcurrentHashMap<String, String>();
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
				//System.out.println("................................................................entered into the condition");
				lorabinarydata = loraserverqueue.poll();
				loradata = new String(lorabinarydata, StandardCharsets.UTF_8);
				//System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ lora data is: " + loradata);
				obj = parser.parse(loradata);
				jsonob = (JSONObject)obj;
				deviceid = jsonob.get("devEUI").toString();
				protodata = jsonob.get("data").toString();
				protobinary = Base64.getDecoder().decode(protodata);
				values = new Values(deviceid, protobinary);
				spoutcollector.emit(values);
				values.clear();
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
		RobertBoschUtils rbutils = new RobertBoschUtils();
		rbutils.queryCatalogueServer();
		rbutils.subscribeToNetworkServer();
		//System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ subscribed to network server");
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "protodata"));
	}

}
