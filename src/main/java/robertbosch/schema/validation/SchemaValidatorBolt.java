package robertbosch.schema.validation;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SchemaValidatorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		outputCollector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
