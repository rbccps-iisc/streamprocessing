package robertbosch.schema.validation;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class ValidationTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("json_spout", new JSONmessagespout());
		builder.setSpout("proto_spout", new NetworkserverSpout());
		builder.setBolt("proto_to_json_bolt", new ProtoConversionBolt()).shuffleGrouping("proto_spout");
		builder.setBolt("validator_bolt", new SchemaValidatorBolt()).shuffleGrouping("proto_to_json_bolt").shuffleGrouping("json_spout");
		
		Config config = new Config();
		
		//running in local mode
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("validation topology", config, builder.createTopology());
		try {
			Thread.sleep(1000000000);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
		
		//running in remote mode
//		try {
//			StormSubmitter.submitTopology("Validation topology", config, builder.createTopology());
//		} catch(InvalidTopologyException invalid) {
//			invalid.printStackTrace();
//		} catch(AlreadyAliveException alive) {
//			alive.printStackTrace();
//		} catch(AuthorizationException auth) {
//			auth.printStackTrace();
//		}
	}
}