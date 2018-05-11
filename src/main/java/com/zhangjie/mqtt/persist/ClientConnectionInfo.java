package com.zhangjie.mqtt.persist;

public class ClientConnectionInfo {
	private String nodeId;
	private long time;
	
	public ClientConnectionInfo(String nodeId, long time) {
		this.nodeId = nodeId;
		this.time = time;
	}

	public String getNodeId() {
		return nodeId;
	}

	public long getTime() {
		return time;
	}
}
