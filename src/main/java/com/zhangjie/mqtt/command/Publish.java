package com.zhangjie.mqtt.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.Client;
import com.zhangjie.mqtt.client.ClientManager;
import com.zhangjie.mqtt.cluster.Cluster;
import com.zhangjie.mqtt.cluster.PublishMessage;
import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.subscribe.ClientIdQos;
import com.zhangjie.mqtt.subscribe.SubscribeInfo;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.messages.MqttPublishMessage;

public class Publish {
	private static final Logger logger = LoggerFactory.getLogger(Publish.class);
	
	private MqttEndpoint endpoint;
	private MqttPublishMessage publish;
	
	public Publish(MqttEndpoint endpoint, MqttPublishMessage message) {
		this.endpoint = endpoint;
		this.publish = message;
	}
	
	public void process() {
		String topic = publish.topicName();
		int publishQos = publish.qosLevel().value();
		
		logger.info("Receive Publish msg from client[{}], topic[{}], qos[{}], message[{}]",
				endpoint.clientIdentifier(), topic, publishQos, new String(publish.payload().getBytes()));
		
		List<ClientIdQos> subscribedClients = SubscribeInfo.getInstance().getSubscribedClients(topic);
		
		if (subscribedClients == null) {
			if (publishQos > 0) {
				endpoint.publishAcknowledge(publish.messageId());
			}
			logger.info("There is no subscribed client");
			return;
		}
		
		//check if need to save message into db.
		boolean needSaveMessage = false;
		if (publishQos > 0) {
			//save publish message if Qos > 0
			needSaveMessage = true;
		}
		
		if (needSaveMessage) {
			Persistence.getInstance().saveMessage(endpoint.clientIdentifier(), topic,
					publish.messageId(), publish.payload().getBytes(), result -> {
						if (result.isSucceeded()) {
							//relay publish message to other nodes
							Cluster.getInstance().relayPublishMessage(new PublishMessage(publish.topicName(), publish.qosLevel().value(),
									result.getResult(), publish.payload().getBytes()));
							
							for (ClientIdQos ciq : subscribedClients) {
								int subscribedQos = ciq.getQos();
								if (publishQos < subscribedQos) {
									subscribedQos = publishQos;
								}
								final int qos = subscribedQos;
								
								Client client = ClientManager.getInstance().getClient(ciq.getClientId());
								if (client == null) {
									continue;
								}
								
								if (qos > 0) {//TODO: need to check 'clean session' flag
									//save client output message list
									Persistence.getInstance().saveClientMessage(client.endpoint().clientIdentifier(),
											result.getResult().intValue(), qos, saveResult -> {
												if (saveResult.isSucceeded()) {
													logger.info("Save and send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
															client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
													client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
													int packetId = client.endpoint().lastMessageId();
													client.savePublishMessage(packetId, result.getResult().intValue(), topic, publish.payload().getBytes(), MqttQoS.valueOf(qos), false, false);
													logger.info("Save publish message, packetId[{}], insertId[{}]", packetId, result.getResult());
												} else {
													logger.error("Failed to save client[{}] message, reason[{}]",
															client.endpoint().clientIdentifier(), saveResult.getCause().getMessage());
												}
											});
								} else {
									//send message to client
									logger.info("Send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
											client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
									client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
								}
							}
						} else {
							logger.error("Failed to save client[{}] message, reason[{}]",
									endpoint.clientIdentifier(), result.getCause().getMessage());
						}
					});
		} else {
			//relay publish message to other nodes
			Cluster.getInstance().relayPublishMessage(new PublishMessage(publish.topicName(), publish.qosLevel().value(),
					-1, publish.payload().getBytes()));
			
			//No need to save message
			for (ClientIdQos ciq : subscribedClients) {
				int subscribedQos = ciq.getQos();
				if (publishQos < subscribedQos) {
					subscribedQos = publishQos;
				}
				final int qos = subscribedQos;
				
				Client client = ClientManager.getInstance().getClient(ciq.getClientId());
				if (client != null) {
					logger.info("Send without save Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
							client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
					client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
				}
			}
		}

		if (publishQos > 0) {
			endpoint.publishAcknowledge(publish.messageId());
		}
	}
}
