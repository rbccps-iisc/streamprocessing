package robertbosch.schema.validation;

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
	String device;
	byte[] data;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		try {
			device = new String(arg0.getBinaryByField("deviceid"), "UTF-8");
			data = arg0.getBinaryByField("protodata");
			ProtobufDeserializer.deserialize(data, SPOUTvalidator.deviceprotoschema.get(device).split("___")[1], 
												SPOUTvalidator.deviceprotoschema.get(device).split("___")[0]);
			
		} catch(UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.outputCollector = arg2;
		RobertBoschUtils utils = new RobertBoschUtils();
		RobertBoschUtils.getPublishChannel();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("jsondata"));
	}

}
