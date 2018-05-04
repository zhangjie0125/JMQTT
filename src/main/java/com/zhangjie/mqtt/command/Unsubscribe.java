package com.zhangjie.mqtt.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.persist.PersistenceCallback;

import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.messages.MqttUnsubscribeMessage;

public class Unsubscribe {
	private static final Logger logger = LoggerFactory.getLogger(Unsubscribe.class);
	
	private MqttEndpoint endpoint;
	private MqttUnsubscribeMessage unsubscribe;
	
	public Unsubscribe(MqttEndpoint endpoint, MqttUnsubscribeMessage message) {
		this.endpoint = endpoint;
		this.unsubscribe = message;
	}
	
	public void process() {
		StringBuilder sb = new StringBuilder();
		for (String t : unsubscribe.topics()) {
			sb.append(t).append(",");
		}
		logger.info("client[{}] unsubscribe info[{}]", endpoint.clientIdentifier(), sb.toString());
		
		Persistence.getInstance().removeClientSubscribe(endpoint.clientIdentifier(), unsubscribe.topics(),
				new PersistenceCallback() {
					@Override
					public void onSucceed(Integer insertId) {
						// ack the subscriptions request
						endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
					}

					@Override
					public void onFail(Throwable t) {
						logger.error("Failed to remove client[{}] subscribe info, close client connection. reason[{}]",
								endpoint.clientIdentifier(), t.getMessage());
						endpoint.close();
					}
				});
	}
}
