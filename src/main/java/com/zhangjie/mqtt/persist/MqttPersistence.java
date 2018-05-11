package com.zhangjie.mqtt.persist;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Vertx;

public interface MqttPersistence {
	void start(Vertx vertx);
	void stop();
	
	void saveClientConnection(String clientId, String nodeId, PersistenceCallback cb);
	void getClientConnection(String clientId, MqttPersistenceHandler<ClientConnectionInfo> handler);
	
	void saveClientSubscribe(String clientId, ArrayList<MqttTopicQos> subscribeInfo, PersistenceCallback cb);
	void removeClientSubscribe(String clientId, List<String> topics, PersistenceCallback cb);
	
	void saveMessage(String clientId, String topic, int packetId, byte[] msg, PersistenceCallback cb);
	void saveClientMessage(String clientId, int msgId, int qos, PersistenceCallback cb);
	void removeClientMessage(String clientId, int msgId, PersistenceCallback cb);
}
