package com.zhangjie.mqtt.client;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.vertx.mqtt.MqttEndpoint;

public class ClientManager {
    private volatile static ClientManager instance;
    private Lock lock;
    private HashMap<String/*ClientId*/, Client> map;

    public static ClientManager getInstance(){
        if(instance == null){
            synchronized (ClientManager.class){
                if(instance == null){
                    instance = new ClientManager();
                }
            }
        }
        return instance;
    }
    
    private ClientManager() {
    	map = new HashMap<>();
    	lock = new ReentrantLock();
    }
    
    public void addNewClient(String clientId, MqttEndpoint endpoint) {
    	lock.lock();
    	
    	if (map.containsKey(clientId)) {
    		Client oldClient = map.get(clientId);
    		System.out.println("Close old client connection");
    		oldClient.endpoint().close();
    		map.replace(clientId, new Client(endpoint));
    	} else {
    		map.put(clientId, new Client(endpoint));
    		System.out.println("Add new client connection for clientId[" + clientId + "]");
    	}
    	lock.unlock();
    }
    
    public void removeClient(String clientId) {
    	lock.lock();
    	
    	if (map.containsKey(clientId)) {
    		//MqttEndpoint endpoint = map.get(clientId);
    		//endpoint.close();
    		map.remove(clientId);
    	}
    	lock.unlock();
    }
    
    public Client getClient(String clientId) {
    	lock.lock();
    	
    	Client client;
    	if (map.containsKey(clientId)) {
    		client = map.get(clientId);
    	} else {
    		client = null;
    	}
    	
    	lock.unlock();
    	return client;
    }
}
