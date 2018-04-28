package com.zhangjie.mqtt.client;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;

class PublishMessage {
	private int msgId;
	private String topic;
	private Buffer payload;
	private MqttQoS qos;
	private boolean isDup;
	private boolean isRetain;
	private int insertId;
	
	public PublishMessage(int msgId, int insertId, String topic, Buffer payload, MqttQoS qos, boolean isDup, boolean isRetain) {
		this.msgId = msgId;
		this.insertId = insertId;
		this.topic = topic;
		this.payload = payload;
		this.qos = qos;
		this.isDup = isDup;
		this.isRetain = isRetain;
	}
	
	public int getMsgId() {
		return msgId;
	}
	public void setMsgId(int msgId) {
		this.msgId = msgId;
	}
	public int getInsertId() {
		return insertId;
	}
	public void setInsertId(int insertId) {
		this.insertId = insertId;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public Buffer getPayload() {
		return payload;
	}
	public void setPayload(Buffer payload) {
		this.payload = payload;
	}
	public MqttQoS getQos() {
		return qos;
	}
	public void setQos(MqttQoS qos) {
		this.qos = qos;
	}
	public boolean isDup() {
		return isDup;
	}
	public void setDup(boolean isDup) {
		this.isDup = isDup;
	}
	public boolean isRetain() {
		return isRetain;
	}
	public void setRetain(boolean isRetain) {
		this.isRetain = isRetain;
	}
}

public class Client {
	private MqttEndpoint endpoint;
	private ArrayList<PublishMessage> sentPublishMsgs;
	private Lock lock;
	
	public Client(MqttEndpoint endpoint) {
		this.endpoint = endpoint;
		sentPublishMsgs = new ArrayList<>();
		lock = new ReentrantLock();
	}
	
	public void savePublishMessage(int msgId, int insertId, String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain) {
		lock.lock();
		sentPublishMsgs.add(new PublishMessage(msgId, insertId, topic, payload, qosLevel, isDup, isRetain));
		lock.unlock();
	}
	
	public int removePublishMessage(int msgId) {
		lock.lock();
		for (PublishMessage msg : sentPublishMsgs) {
			if (msg.getMsgId() == msgId) {
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
