package com.zhangjie.mqtt.persist;

/**
 * @author admin
 *
 */
public class MqttTopicQos {
	private int qos;
	private String topic;

	public MqttTopicQos(String topicName, int value) {
		topic = topicName;
		qos = value;
	}
	public int getQos() {
		return qos;
	}
	public void setQos(int qos) {
		this.qos = qos;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
}
