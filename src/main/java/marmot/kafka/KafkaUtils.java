package marmot.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.errors.TopicExistsException;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.utils.ZkUtils;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaUtils {
	private KafkaUtils() {
		throw new AssertionError();
	}
	
	public static String getZooKeeperHosts(Configuration conf) {
		String zkHosts = conf.get("marmot.zookeeper.hosts", null);
		if ( zkHosts == null ) {
			throw new IllegalStateException("ZooKeeper hosts is missing: "
											+ "prop='marmot.zookeeper.hosts'");
		}
		
		return zkHosts;
	}
	
	public static String getKafkaBrokerList(Configuration conf) {
		String brokers = conf.get("marmot.kafka.brokers", null);
		if ( brokers == null ) {
			throw new IllegalStateException("ZooKeeper hosts is missing: "
											+ "prop='marmot.kafka.brokers'");
		}
		
		return brokers;
	}
	
	public static void createKafkaTopic(String zkHosts, String topic, int nparts, int nreps,
										boolean force) {
		if ( !force ) {
			_createKafkaTopic(zkHosts, topic, nparts, nreps);
		}
		else {
			RuntimeException error = null;
			for ( int i =0; i < 10; ++i ) {
				try {
					_createKafkaTopic(zkHosts, topic, nparts, nreps);
					return;
				}
				catch ( TopicExistsException | TopicAlreadyMarkedForDeletionException e ) {
					error = e;
					deleteKafkaTopic(zkHosts, topic);
					
					Try.run(() -> Thread.sleep(1000));
				}
			}
			if ( error != null ) {
				throw error;
			}
			else {
				throw new RuntimeException("fails to delete KafkaTopic: " + topic);
			}
		}
	}
	
	public static void deleteKafkaTopic(String zkHosts, String topic) {
		ZkClient client = new ZkClient(zkHosts);
		try {
			String topicPath = ZkUtils.getTopicPath(topic);
			client.deleteRecursive(topicPath);
		}
		finally {
			client.close();
		}
	}
	
	private static void _createKafkaTopic(String zkHosts, String topic, int nparts, int nreps) {
		int sessionTimeout = 15 * 1000;
		int connectionTimeout = 10 * 1000;
		
		ZkUtils utils = null;
		try {
			utils = ZkUtils.apply(zkHosts, sessionTimeout, connectionTimeout, false);
			AdminUtils.createTopic(utils, topic, nparts, nreps, new Properties(),
									RackAwareMode.Safe$.MODULE$);
		}
		finally {
			if ( utils != null ) {
				utils.close();
			}
		}
	}
}
