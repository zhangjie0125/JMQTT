package com.zhangjie.mqtt.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.ClientManager;

import io.vertx.mqtt.MqttEndpoint;

public class CloseConnection {
	private static final Logger logger = LoggerFactory.getLogger(CloseConnection.class);
	private MqttEndpoint endpoint;
	
	public CloseConnection(MqttEndpoint endpoint) {
		this.endpoint = endpoint;
	}
	
	public void process() {
		logger.info("client [{}] closed connection", endpoint.clientIdentifier());
		ClientManager.getInstance().removeClient(endpoint.clientIdentifier());
	}
}
