package com.zhangjie.mqtt;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	DeploymentOptions options = new DeploymentOptions().setInstances(1);
    	Vertx.vertx().deployVerticle("com.zhangjie.mqtt.MqttVerticle", options);
    }
}
