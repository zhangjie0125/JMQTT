package com.zhangjie.mqtt.cluster;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.Client;
import com.zhangjie.mqtt.client.ClientManager;
import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.persist.PersistenceCallback;
import com.zhangjie.mqtt.subscribe.ClientIdQos;
import com.zhangjie.mqtt.subscribe.SubscribeInfo;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;

public class Publish {
	private PublishMessage message;
	
	private static final Logger logger = LoggerFactory.getLogger(Publish.class);

	public Publish(PublishMessage message) {
		super();
		this.message = message;
	}
	
	public void process() {
		logger.info("Process Publish command: topic[{}], qos[{}], payload[{}], insertId[{}]",
				message.getTopic(), message.getQos(), message.getPayload(), message.getInsertId());
		
		List<ClientIdQos> subscribedClients = SubscribeInfo.getInstance().getSubscribedClients(message.getTopic());
		if (subscribedClients == null) {
			logger.info("There is no subscribed client");
			return;
		}
		
		for (ClientIdQos ciq : subscribedClients) {
			int subscribedQos = ciq.getQos();
			if (message.getQos() < subscribedQos) {
				subscribedQos = message.getQos();
			}
			final int qos = subscribedQos;
			
			Client client = ClientManager.getInstance().getClient(ciq.getClientId());
			if (client == null) {
				continue;
			}
			
			if (qos > 0) {//TODO: need to check 'clean session' flag
				//save client output message list
				Persistence.getInstance().saveClientMessage(client.endpoint().clientIdentifier(),
						message.getInsertId(), qos, new PersistenceCallback() {
					@Override
					public void onSucceed(Integer id) {
						logger.info("Save and send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
								client.endpoint().clientIdentifier(), message.getTopic(), qos, new String(message.getPayload()));
						client.endpoint().publish(message.getTopic(), Buffer.buffer(message.getPayload()), MqttQoS.valueOf(qos), false, false);
						int packetId = client.endpoint().lastMessageId();
						client.savePublishMessage(packetId, message.getInsertId(), message.getTopic(), message.getPayload(), MqttQoS.valueOf(qos), false, false);
						logger.info("Save publish message, packetId[{}], insertId[{}]", packetId, message.getInsertId());
					}
					
					@Override
					public void onFail(Throwable t) {
						logger.error("Failed to save client[{}] message, reason[{}]",
								client.endpoint().clientIdentifier(), t.getMessage());
					}
				});
			} else {
				//send message to client
				logger.info("Send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
						client.endpoint().clientIdentifier(), message.getTopic(), qos, new String(message.getPayload()));
				client.endpoint().publish(message.getTopic(), Buffer.buffer(message.getPayload()), MqttQoS.valueOf(qos), false, false);
			}
		}
	}
}
