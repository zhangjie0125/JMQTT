package com.zhangjie.mqtt.persist;

public class MqttPersistenceResult<T> {
	private T result;
	private Throwable e;
	private boolean succeeded;
	
	public boolean isSucceeded() {
		return succeeded;
	}
	
	public void setResult(T result) {
		succeeded = true;
		this.result = result;
	}

	public T getResult() {
		return result;
	}
	
	public void setCause(Throwable t) {
		succeeded = false;
		e = t;
	}
	
	public Throwable getCause() {
		return e;
	}
}
