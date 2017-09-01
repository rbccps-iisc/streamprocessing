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
		builder.setSpout("spout", new SchemaBrokerSpout());
		builder.setBolt("bolt", new SchemaVerifyBolt()).shuffleGrouping("spout");
		
		Config config = new Config();
//		LocalCluster cluster = new LocalCluster();
//		
//		cluster.submitTopology("schema_verification", config, builder.createTopology());
//		try {
//			Thread.sleep(100000);
//		} catch(InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		cluster.shutdown();
		
		try {
			StormSubmitter.submitTopology("schema_verification", config, builder.createTopology());
		} catch(InvalidTopologyException invalid) {
			invalid.printStackTrace();
		} catch(AlreadyAliveException alive) {
			alive.printStackTrace();
		} catch(AuthorizationException auth) {
			auth.printStackTrace();
		}
	}
}