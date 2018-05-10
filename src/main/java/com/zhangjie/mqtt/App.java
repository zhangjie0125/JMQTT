package com.zhangjie.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.cluster.Cluster;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;

/**
 * Hello world!
 *
 */
public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) {
		JsonObject zkConfig = new JsonObject();
		zkConfig.put("zookeeperHosts", "10.10.10.10");
		zkConfig.put("rootPath", "io.vertx");
		zkConfig.put("retry", new JsonObject().put("initialSleepTime", 3000).put("maxTimes", 3));

		ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
		VertxOptions options = new VertxOptions().setClusterManager(mgr).setClusterHost("10.10.10.10");

		Vertx.clusteredVertx(options, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				
				Cluster.getInstance().setVertx(vertx);
				
				DeploymentOptions deployOptions = new DeploymentOptions().setInstances(10);
				vertx.deployVerticle("com.zhangjie.mqtt.MqttVerticle", deployOptions);
			} else {
				logger.error("Failed to create clustered vertx. reason:{}", res.cause().getMessage());
			}
		});
	}
}
