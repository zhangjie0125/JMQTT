package com.zhangjie.mqtt.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;

public class Cluster {
	private volatile static Cluster instance;
	private Vertx vertx;
	
	private static final Logger logger = LoggerFactory.getLogger(Cluster.class);
	
    public static Cluster getInstance(){
        if(instance == null){
            synchronized (Cluster.class){
                if(instance == null){
                    instance = new Cluster();
                }
            }
        }
        return instance;
    }
    
    private Cluster() {
    	
    }
    
    public void setVertx(Vertx vertx) {
    	this.vertx = vertx;
    	this.vertx.eventBus().registerDefaultCodec(PublishMessage.class, new PublishMessageCodec());
    	
    	//setup callback function to process Cluster Publish message
    	processPublishMessage();
    }
    
    public void relayPublishMessage(PublishMessage m) {
		//delay publish message to other nodes
    	logger.info("Send cluster publish message: topic[{}], qos[{}], message[{}], insertId[{}]",
    			m.getTopic(), m.getQos(), m.getPayload(), m.getInsertId());
		vertx.eventBus().publish("Cluster-Publish-Message", m);
    }
    
    private void processPublishMessage() {
    	vertx.eventBus().consumer("Cluster-Publish-Message", message -> {
    		PublishMessage m = (PublishMessage)message.body();
    		Publish cmd = new Publish(m);
    		cmd.process();
    	});
    }
}
