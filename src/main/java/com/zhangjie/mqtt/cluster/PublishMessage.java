package com.zhangjie.mqtt.cluster;

public class PublishMessage {
	private String topic;
	private int qos;
	private int insertId;
	private byte[] payload;
	
	public PublishMessage(String topic, int qos, int insertId, byte[] payload) {
		super();
		this.topic = topic;
		this.qos = qos;
		this.insertId = insertId;
		this.payload = payload;
	}

	public String getTopic() {
		return topic;
	}
	public int getQos() {
		return qos;
	}
	public int getInsertId() {
		return insertId;
	}
	public byte[] getPayload() {
		return payload;
	}
}
