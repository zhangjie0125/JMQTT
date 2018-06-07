package com.zhangjie.mqtt.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.Client;
import com.zhangjie.mqtt.client.ClientManager;
import com.zhangjie.mqtt.persist.Persistence;

import io.vertx.mqtt.MqttEndpoint;

public class PubAck {
	private static final Logger logger = LoggerFactory.getLogger(PubAck.class);
	
	private MqttEndpoint endpoint;
	private Integer id;
	
	public PubAck(MqttEndpoint endpoint, Integer id) {
		this.endpoint = endpoint;
		this.id = id;
	}
	
	public void process() {
		Client client = ClientManager.getInstance().getClient(endpoint.clientIdentifier());
		if (client != null) {
			int insertId = client.removePublishMessage(id);
			logger.info("PubAck client[{}], packetId[{}], insertId[{}]",
					endpoint.clientIdentifier(), id, insertId);
			Persistence.getInstance().removeClientMessage(endpoint.clientIdentifier(), insertId,
					result -> {
						if (result.isSucceeded()) {
							//nothing need to do here
						} else {
							logger.error("Failed to remove client[{}] message, reason[{}]",
									endpoint.clientIdentifier(), result.getCause().getMessage());
						}
					});
		}
	}
}
