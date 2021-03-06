package com.zhangjie.mqtt.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.ClientManager;
import com.zhangjie.mqtt.cluster.CloseClientMessage;
import com.zhangjie.mqtt.cluster.Cluster;
import com.zhangjie.mqtt.persist.ClientConnectionInfo;
import com.zhangjie.mqtt.persist.Persistence;

import io.vertx.mqtt.MqttEndpoint;

public class Connect {
	private static final Logger logger = LoggerFactory.getLogger(Connect.class);
	private MqttEndpoint endpoint;
	
	public Connect(MqttEndpoint endpoint) {
		this.endpoint = endpoint;
	}
	
	public void process() {
		if (endpoint.auth() == null) {
			logger.info("MQTT client [{}] request to connect, clean session[{}], keepAliveTime[{}]",
					endpoint.clientIdentifier(), endpoint.isCleanSession(), endpoint.keepAliveTimeSeconds());
		} else {
			logger.info("MQTT client [{}] request to connect, clean session[{}], username[{}], password[{}], keepAliveTime[{}]",
					endpoint.clientIdentifier(), endpoint.isCleanSession(),
					endpoint.auth().userName(), endpoint.auth().password(), endpoint.keepAliveTimeSeconds());
		}
		/*if (endpoint.will() != null) {
			System.out.println("[will topic = " + endpoint.will().willTopic() + " msg = "
					+ endpoint.will().willMessage() + " QoS = " + endpoint.will().willQos() + " isRetain = "
					+ endpoint.will().isWillRetain() + "]");
		}*/
		
		Persistence.getInstance().getClientConnection(endpoint.clientIdentifier(), r -> {
			if (r.isSucceeded()) {
				ClientConnectionInfo info = r.getResult();
				if (info.getTime() == 0) {
					//this is the first time that client connects to me
				} else {
					String nodeId = info.getNodeId();
					long time = info.getTime();
					logger.info("get client[{}] connection nodeId[{}], time[{}]", endpoint.clientIdentifier(), nodeId, time);
					if (!nodeId.equals(Cluster.getInstance().getNodeId())) {
						Cluster.getInstance().relayCloseClientMessage(nodeId,
								new CloseClientMessage(endpoint.clientIdentifier(), time));
					}
				}
			} else {
				logger.error("Failed to get client[{}] connection, reason:{}", endpoint.clientIdentifier(), r.getCause().getMessage());
			}

			Persistence.getInstance().saveClientConnection(endpoint.clientIdentifier(), Cluster.getInstance().getNodeId(),
					result -> {
						if (result.isSucceeded()) {
							// accept connection from the remote client
							endpoint.accept(true);
							ClientManager.getInstance().addNewClient(endpoint.clientIdentifier(), endpoint);
						} else {
							logger.error("Failed to save client[{}] connection info, close client connection. reason[{}]",
									endpoint.clientIdentifier(), result.getCause().getMessage());
							endpoint.close();
						}
					});
		});
	}
}
