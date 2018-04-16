package robertbosch.schema.validation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import robertbosch.utils.ProtobufDeserializer;
import robertbosch.utils.RobertBoschUtils;

public class ProtoConversionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String device, jsondata;
	byte[] data;
	private RobertBoschUtils utils;
	public static Set<String> protoURLs;
	
	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		device = arg0.getStringByField("deviceid");
		data = arg0.getBinaryByField("protodata");
		//System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ device id is:" + device);
		//System.out.println("%%%%%%%%%%%%%%%%%%  key set of protoschema map:" + NetworkserverSpout.deviceprotoschema.keySet());
		
		if(data != null) {
			
			if(!RobertBoschUtils.catalogue.contains(device)) {
				System.out.println("#################################################### querying catalogue from the bolt validator......................");
				utils.queryCatalogueServer();
			}
			
			System.out.println("******************** link: " + NetworkserverSpout.deviceprotoschema.get(device).split("___")[0]);
			System.out.println("~~~~~~~~~~~~~~~~~~~~ mainmessage: " + NetworkserverSpout.deviceprotoschema.get(device).split("___")[1]);
			//deserialize method args:(byte[] buffer, String message, String url) and split string is url__message
			jsondata = ProtobufDeserializer.deserialize(data, NetworkserverSpout.deviceprotoschema.get(device).split("___")[1], 
															NetworkserverSpout.deviceprotoschema.get(device).split("___")[0]);
			//System.out.println("#################################$$$******************%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% data is: " + jsondata);
			
			Values values = new Values(device, jsondata);
			outputCollector.emit(values);
			values.clear();
			
//			try {
//			
//				RobertBoschUtils.publishchannel.queueDeclare(RobertBoschUtils.pubTopic, false, false, false, null);
//				RobertBoschUtils.publishchannel.basicPublish("", RobertBoschUtils.pubTopic, null, jsondata.getBytes());
//			
//			} catch(IOException e) {
//			e.printStackTrace();
//			}
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		utils = new RobertBoschUtils();
		utils.getPublishChannel();
		protoURLs = new HashSet<String>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("deviceid", "jsondata"));
	}

}
