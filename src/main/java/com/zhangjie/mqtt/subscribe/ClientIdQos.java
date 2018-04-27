package com.zhangjie.mqtt.subscribe;

public class ClientIdQos {
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public int getQos() {
		return qos;
	}
	public void setQos(int qos) {
		this.qos = qos;
	}
	public ClientIdQos(String clientId, int qos) {
		super();
		this.clientId = clientId;
		this.qos = qos;
	}
	private String clientId;
	private int qos;
	
	
}
