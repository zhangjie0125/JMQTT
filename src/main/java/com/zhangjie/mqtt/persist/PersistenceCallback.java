package com.zhangjie.mqtt.persist;

public interface PersistenceCallback {
	void onSucceed(Integer insertId);
	void onFail(Throwable t);
}
