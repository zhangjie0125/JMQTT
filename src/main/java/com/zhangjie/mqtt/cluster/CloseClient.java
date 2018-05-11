package com.zhangjie.mqtt.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.ClientManager;

public class CloseClient {
	private CloseClientMessage message;
	
	private static final Logger logger = LoggerFactory.getLogger(CloseClient.class);
	
	public CloseClient(CloseClientMessage message) {
		this.message = message;
	}
	
	public void process() {
		logger.info("Process CloseClient command, clientId[{}], time[{}]", message.getClientId(), message.getTime());
		ClientManager.getInstance().removeClient(message.getClientId());
	}
}
