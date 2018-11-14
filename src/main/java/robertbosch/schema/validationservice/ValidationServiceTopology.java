package robertbosch.schema.validationservice;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ValidationServiceTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("json_spout", new RabbitMQConsumerSpout());
		builder.setBolt("validator_bolt", new SchemaValidatorBolt()).shuffleGrouping("json_spout");
		builder.setBolt("counter_bolt", new CounterBolt()).fieldsGrouping("validator_bolt","counterStream",new Fields("deviceid"));
		builder.setBolt("validatedMessagePublisher_bolt",new ValidatedMessagePublisherBolt()).shuffleGrouping("validator_bolt","publishValidatedStream");
		Config config = new Config();

		//running in local mode
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("validationServiceTopology", config, builder.createTopology());
		Config conf = new Config();
		conf.setNumWorkers(2);


		try {
			StormSubmitter.submitTopology(args[0],
					config, builder.createTopology());
		} catch(AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
//		cluster.shutdown();

		//running in remote modes
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
