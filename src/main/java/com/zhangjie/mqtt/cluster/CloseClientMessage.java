package com.zhangjie.mqtt.cluster;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class CloseClientMessage {
	private String clientId;
	private long time;
	public CloseClientMessage(String clientId, long time) {
		super();
		this.clientId = clientId;
		this.time = time;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
}

class CloseClientMessageCodec implements MessageCodec<CloseClientMessage, CloseClientMessage> {

	@Override
	public void encodeToWire(Buffer buffer, CloseClientMessage s) {
		// Easiest ways is using JSON object
	    JsonObject jsonToEncode = new JsonObject();
	    jsonToEncode.put("clientId", s.getClientId());
	    jsonToEncode.put("time", s.getTime());

	    // Encode object to string
	    String jsonToStr = jsonToEncode.encode();

	    // Length of JSON: is NOT characters count
	    int length = jsonToStr.getBytes().length;

	    // Write data into given buffer
	    buffer.appendInt(length);
	    buffer.appendString(jsonToStr);
	}

	@Override
	public CloseClientMessage decodeFromWire(int pos, Buffer buffer) {
		// My custom message starting from this *position* of buffer
	    int _pos = pos;

	    // Length of JSON
	    int length = buffer.getInt(_pos);

	    // Get JSON string by it`s length
	    // Jump 4 because getInt() == 4 bytes
	    String jsonStr = buffer.getString(_pos+=4, _pos+=length);
	    JsonObject contentJson = new JsonObject(jsonStr);

	    // Get fields
	    String clientId = contentJson.getString("clientId");
	    Long time = contentJson.getLong("time");

	    // We can finally create custom message object
	    return new CloseClientMessage(clientId, time);
	}

	@Override
	public CloseClientMessage transform(CloseClientMessage s) {
		return s;
	}

	@Override
	public String name() {
		return this.getClass().getSimpleName();
	}

	@Override
	public byte systemCodecID() {
		// Always -1
	    return -1;
	}

}
