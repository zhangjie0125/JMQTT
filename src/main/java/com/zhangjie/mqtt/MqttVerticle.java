package com.zhangjie.mqtt;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.AbstractVerticle;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttVerticle extends AbstractVerticle {
	@Override
	public void start() {
		MqttServerOptions options = new MqttServerOptions().setPort(50000);

		MqttServer mqttServer = MqttServer.create(vertx, options);
		mqttServer.endpointHandler(endpoint -> {

			initClient(endpoint);
			initClientSubscribe(endpoint);
		})
		  .listen(ar -> {

		    if (ar.succeeded()) {

		      System.out.println("MQTT server is listening on port " + ar.result().actualPort());
		    } else {

		      System.out.println("Error on starting the server");
		      ar.cause().printStackTrace();
		    }
		  });
	}
	
	private void initClient(MqttEndpoint endpoint) {
		  // shows main connect info
		  System.out.println("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = " + endpoint.isCleanSession());
		
		  if (endpoint.auth() != null) {
		    System.out.println("[username = " + endpoint.auth().userName() + ", password = " + endpoint.auth().password() + "]");
		  }
		  if (endpoint.will() != null) {
		    System.out.println("[will topic = " + endpoint.will().willTopic() + " msg = " + endpoint.will().willMessage() +
		    		" QoS = " + endpoint.will().willQos() + " isRetain = " + endpoint.will().isWillRetain() + "]");
		  }
		
		  System.out.println("[keep alive timeout = " + endpoint.keepAliveTimeSeconds() + "]");
		
		  // accept connection from the remote client
		  endpoint.accept(false);
	}
	
	private void initClientSubscribe(MqttEndpoint endpoint) {
		endpoint.subscribeHandler(subscribe -> {

		  List<MqttQoS> grantedQosLevels = new ArrayList<>();
		  for (MqttTopicSubscription s: subscribe.topicSubscriptions()) {
		    System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
		    grantedQosLevels.add(s.qualityOfService());
		  }
		  // ack the subscriptions request
		  endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);

		});
		
		endpoint.unsubscribeHandler(unsubscribe -> {

		  for (String t: unsubscribe.topics()) {
		    System.out.println("Unsubscription for " + t);
		  }
		  // ack the subscriptions request
		  endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
		});
	}
}
