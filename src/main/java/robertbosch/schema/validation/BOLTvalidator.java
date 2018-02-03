package robertbosch.schema.validation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import robertbosch.utils.ProtobufDeserializer;
import robertbosch.utils.RobertBoschUtils;

public class BOLTvalidator extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	String device, jsondata;
	byte[] data;
	private RobertBoschUtils utils;
	
	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		device = arg0.getStringByField("deviceid");
		data = arg0.getBinaryByField("protodata");
		
		if(!RobertBoschUtils.catalogue.contains(device)) {
			utils.queryCatalogurServer();
		}
		
		jsondata = ProtobufDeserializer.deserialize(data, NetworkserverSpout.deviceprotoschema.get(device).split("___")[0], 
											NetworkserverSpout.deviceprotoschema.get(device).split("___")[1]);
		System.out.println("#################################$$$******************%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% data is: " + jsondata);
		
//		try {
//			
//			RobertBoschUtils.publishchannel.queueDeclare(RobertBoschUtils.pubTopic, false, false, false, null);
//			RobertBoschUtils.publishchannel.basicPublish("", RobertBoschUtils.pubTopic, null, jsondata.getBytes());
//			
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		utils = new RobertBoschUtils();
		utils.getPublishChannel();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("jsondata"));
	}

}
