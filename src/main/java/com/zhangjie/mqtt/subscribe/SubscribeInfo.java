package com.zhangjie.mqtt.subscribe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.MqttVerticle;
import com.zhangjie.mqtt.persist.MqttTopicQos;

public class SubscribeInfo {
    private volatile static SubscribeInfo instance;
    private Lock lock;
    private HashMap<String/*topic*/, List<ClientIdQos>> map;
    
    private static final Logger logger = LoggerFactory.getLogger(SubscribeInfo.class);

    public static SubscribeInfo getInstance(){
        if(instance == null){
            synchronized (SubscribeInfo.class){
                if(instance == null){
                    instance = new SubscribeInfo();
                }
            }
        }
        return instance;
    }
    
    private SubscribeInfo() {
    	map = new HashMap<>();
    	lock = new ReentrantLock();
    }
    
    public void addNewSubscribeInfo(String clientId, String topic, int qos) {
    	logger.info("Add subscribe info, client[{}], topic[{}], qos[{}]", clientId, topic, qos);
    	
    	ClientIdQos c = new ClientIdQos(clientId, qos);
    	
    	lock.lock();
    	List<ClientIdQos> subscribedClients = map.get(topic);
    	if (subscribedClients == null) {
    		List<ClientIdQos> newClientList = new ArrayList<>();
    		newClientList.add(c);
    		map.put(topic, newClientList);
    	} else {
    		subscribedClients.add(c);
    	}
    	lock.unlock();
    }
    
    public void addNewSubscribeInfos(String clientId, List<MqttTopicQos> info) {
    	StringBuilder sb = new StringBuilder();
    	lock.lock();
    	
    	for (MqttTopicQos tq : info) {
    		sb.append(tq.getTopic()).append(":").append(tq.getQos()).append(",");
    		
    		ClientIdQos ciq = new ClientIdQos(clientId, tq.getQos());
        	List<ClientIdQos> subscribedClients = map.get(tq.getTopic());
        	if (subscribedClients == null) {
        		List<ClientIdQos> newClientList = new ArrayList<>();
        		newClientList.add(ciq);
        		map.put(tq.getTopic(), newClientList);
        	} else {
        		subscribedClients.add(ciq);
        	}
    	}
    	
    	lock.unlock();
    	
    	logger.info("Add subscribe info, client[{}], topic-qos[{}]", clientId, sb.toString());
    }
    
    public List<ClientIdQos> getSubscribedClients(String topic) {
    	lock.lock();
    	List<ClientIdQos> subscribedClients = map.get(topic);
    	lock.unlock();
    	return subscribedClients;
    }
}
