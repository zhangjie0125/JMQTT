package com.zhangjie.mqtt.persist;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Vertx;

public interface MqttPersistence {
	void start(Vertx vertx);
	void stop();
	
	void saveClientConnection(String clientId, String nodeId, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
	void getClientConnection(String clientId, MqttPersistenceHandler<MqttPersistenceResult<ClientConnectionInfo>> handler);
	
	void saveClientSubscribe(String clientId, ArrayList<MqttTopicQos> subscribeInfo, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
	void removeClientSubscribe(String clientId, List<String> topics, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
	
	void saveMessage(String clientId, String topic, int packetId, byte[] msg, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
	void saveClientMessage(String clientId, int msgId, int qos, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
	void removeClientMessage(String clientId, int msgId, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler);
}
