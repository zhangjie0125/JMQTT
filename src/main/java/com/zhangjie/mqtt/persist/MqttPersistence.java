package com.zhangjie.mqtt.persist;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Vertx;

public interface MqttPersistence {
	void start(Vertx vertx);
	void stop();
	
	void saveClientConnection(String clientId, String nodeId, SaveInfoCallback cb);
	
	void saveClientSubscribe(String clientId, ArrayList<MqttTopicQos> subscribeInfo, SaveInfoCallback cb);
	void removeClientSubscribe(String clientId, List<String> topics, SaveInfoCallback cb);
	
	void saveMessage(String clientId, String topic, int packetId, byte[] msg, SaveInfoCallback cb);
	void saveClientMessage(String clientId, int msgId, int qos, SaveInfoCallback cb);
	void removeClientMessage(String clientId, int msgId, SaveInfoCallback cb);
}
