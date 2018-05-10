package com.zhangjie.mqtt.config;

public class ConfigException extends Exception {

	private static final long serialVersionUID = -2839616249966823671L;

	public ConfigException() {
        super();
    }
 
    public ConfigException(String message) {
        super(message);
    }
 
    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
 
    public ConfigException(Throwable cause) {
        super(cause);
    }
}
