package com.zhangjie.mqtt.cluster;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class PublishMessageCodec implements MessageCodec<PublishMessage, PublishMessage> {

	@Override
	public void encodeToWire(Buffer buffer, PublishMessage s) {
		// Easiest ways is using JSON object
	    JsonObject jsonToEncode = new JsonObject();
	    jsonToEncode.put("topic", s.getTopic());
	    jsonToEncode.put("qos", s.getQos());
	    jsonToEncode.put("insertId", s.getInsertId());
	    jsonToEncode.put("payload", s.getPayload());

	    // Encode object to string
	    String jsonToStr = jsonToEncode.encode();

	    // Length of JSON: is NOT characters count
	    int length = jsonToStr.getBytes().length;

	    // Write data into given buffer
	    buffer.appendInt(length);
	    buffer.appendString(jsonToStr);
	}

	@Override
	public PublishMessage decodeFromWire(int pos, Buffer buffer) {
		// My custom message starting from this *position* of buffer
	    int _pos = pos;

	    // Length of JSON
	    int length = buffer.getInt(_pos);

	    // Get JSON string by it`s length
	    // Jump 4 because getInt() == 4 bytes
	    String jsonStr = buffer.getString(_pos+=4, _pos+=length);
	    JsonObject contentJson = new JsonObject(jsonStr);

	    // Get fields
	    String topic = contentJson.getString("topic");
	    Integer qos = contentJson.getInteger("qos");
	    Integer insertId = contentJson.getInteger("insertId");
	    byte[] payload = contentJson.getBinary("payload");

	    // We can finally create custom message object
	    return new PublishMessage(topic, qos, insertId, payload);
	}

	@Override
	public PublishMessage transform(PublishMessage s) {
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
