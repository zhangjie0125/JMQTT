package com.zhangjie.mqtt.persist;

public class Persistence {
    private volatile static MqttPersistence instance;

    public static MqttPersistence getInstance(){
        if(instance == null){
            synchronized (MqttPersistence.class){
                if(instance == null){
                    instance = new MysqlPersistence();
                    instance.start();
                }
            }
        }
        return instance;
    }
}
