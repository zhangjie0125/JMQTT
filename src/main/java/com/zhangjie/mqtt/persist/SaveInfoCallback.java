package com.zhangjie.mqtt.persist;

public interface SaveInfoCallback {
	void onSucceed(Integer insertId);
	void onFail();
}
