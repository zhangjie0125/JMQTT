package com.zhangjie.mqtt.client;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttEndpoint;

class CachePublishMessage {
	private String topic;
	private int qos;
	private int packetId;
	private boolean isDup;
	private boolean isRetain;
	private byte[] payload;
	private int insertId;
	
	public CachePublishMessage(String topic, int qos, int packetId, boolean isDup, boolean isRetain, byte[] payload, int insertId) {
		this.topic = topic;
		this.qos = qos;
		this.packetId = packetId;
		this.isDup = isDup;
		this.isRetain = isRetain;
		this.payload = payload;
		this.insertId = insertId;
	}

	public String getTopic() {
		return topic;
	}

	public int getQos() {
		return qos;
	}

	public int getPacketId() {
		return packetId;
	}

	public boolean isDup() {
		return isDup;
	}

	public boolean isRetain() {
		return isRetain;
	}

	public byte[] getPayload() {
		return payload;
	}

	public int getInsertId() {
		return insertId;
	}
}


public class Client {
	private MqttEndpoint endpoint;
	private ArrayList<CachePublishMessage> sentPublishMsgs;
	private Lock lock;
	
	public Client(MqttEndpoint endpoint) {
		this.endpoint = endpoint;
		sentPublishMsgs = new ArrayList<>();
		lock = new ReentrantLock();
	}
	
	public void savePublishMessage(int packetId, int insertId, String topic, byte[] payload, MqttQoS qosLevel, boolean isDup, boolean isRetain) {
		lock.lock();
		sentPublishMsgs.add(new CachePublishMessage(topic, qosLevel.value(), packetId, isDup, isRetain, payload, insertId));
		lock.unlock();
	}
	
	public int removePublishMessage(int msgId) {
		lock.lock();
		for (CachePublishMessage msg : sentPublishMsgs) {
			if (msg.getPacketId() == msgId) {
				sentPublishMsgs.remove(msg);
				lock.unlock();
				return msg.getInsertId();
			}
		}
		lock.unlock();
		return 0;
	}
	
	public MqttEndpoint endpoint() {
		return endpoint;
	}
	
	public void close() {
		try {
			endpoint.close();
		} catch (IllegalStateException e) {
			// client is closed, no need to deal with this exception
		}
	}
}
