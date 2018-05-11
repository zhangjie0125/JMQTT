package com.zhangjie.mqtt.persist;

@FunctionalInterface
public interface MqttPersistenceHandler<E> {
	void handle(E event);
}
