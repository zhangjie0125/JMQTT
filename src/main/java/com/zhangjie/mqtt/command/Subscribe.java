package com.zhangjie.mqtt.command;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.persist.MqttTopicQos;
import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.persist.PersistenceCallback;
import com.zhangjie.mqtt.subscribe.SubscribeInfo;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.messages.MqttSubscribeMessage;

public class Subscribe {
	private static final Logger logger = LoggerFactory.getLogger(Subscribe.class);
	
	private MqttEndpoint endpoint;
	private MqttSubscribeMessage subscribe;
	
	public Subscribe(MqttEndpoint endpoint, MqttSubscribeMessage message) {
		this.endpoint = endpoint;
		this.subscribe = message;
	}
	
	public void process() {
		ArrayList<MqttTopicQos> subscribeInfo = new ArrayList<>();
		List<MqttQoS> grantedQosLevels = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {
			sb.append(s.topicName()).append(":").append(s.qualityOfService().value()).append(",");

			grantedQosLevels.add(s.qualityOfService());
			
			subscribeInfo.add(new MqttTopicQos(s.topicName(), s.qualityOfService().value()));
		}
		logger.info("Client[{}] subscribe info[{}]", endpoint.clientIdentifier(), sb.toString());
		
		Persistence.getInstance().saveClientSubscribe(endpoint.clientIdentifier(), subscribeInfo,
				new PersistenceCallback() {
					@Override
					public void onSucceed(Integer insertId) {
						// ack the subscriptions request
						endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
						SubscribeInfo.getInstance().addNewSubscribeInfos(endpoint.clientIdentifier(),
								subscribeInfo);
					}

					@Override
					public void onFail(Throwable t) {
						logger.error("Failed to save client[{}] subscribe info, close client connection. reason[{}]",
								endpoint.clientIdentifier(), t.getMessage());
						endpoint.close();
					}
				});
	}
}
