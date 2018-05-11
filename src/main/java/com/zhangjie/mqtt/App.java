package com.zhangjie.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.cluster.Cluster;
import com.zhangjie.mqtt.config.Config;
import com.zhangjie.mqtt.config.ConfigException;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;

/**
 * App class
 *
 */
public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) {
		try {
			Config.getInstance().loadConfig("conf/mqtt.xml");
		} catch (ConfigException e) {
			logger.error("Failed to read config, reason[{}]", e.getMessage());
			return;
		}
		
		JsonObject zkConfig = new JsonObject();
		zkConfig.put("zookeeperHosts", Config.getInstance().getZkUrl());
		zkConfig.put("rootPath", Config.getInstance().getZkRootPath());
		zkConfig.put("retry", new JsonObject().put("initialSleepTime", 3000).put("maxTimes", 3));

		ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
		VertxOptions options = new VertxOptions().setClusterManager(mgr).setClusterHost(Config.getInstance().getClusterHost());

		Vertx.clusteredVertx(options, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				
				Cluster.getInstance().setNodeId(mgr.getNodeID());
				Cluster.getInstance().setVertx(vertx);
				logger.info("Node ID:{}", Cluster.getInstance().getNodeId());
				
				DeploymentOptions deployOptions = new DeploymentOptions().setInstances(10);
				vertx.deployVerticle("com.zhangjie.mqtt.vertx.MqttVerticle", deployOptions);
			} else {
				logger.error("Failed to create clustered vertx. reason:{}", res.cause().getMessage());
			}
		});
	}
}
