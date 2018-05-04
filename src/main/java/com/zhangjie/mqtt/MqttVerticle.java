package com.zhangjie.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.command.CloseConnection;
import com.zhangjie.mqtt.command.Connect;
import com.zhangjie.mqtt.command.PubAck;
import com.zhangjie.mqtt.command.Publish;
import com.zhangjie.mqtt.command.Subscribe;
import com.zhangjie.mqtt.command.Unsubscribe;
import com.zhangjie.mqtt.persist.Persistence;

import io.vertx.core.AbstractVerticle;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;

public class MqttVerticle extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(MqttVerticle.class);
	
	@Override
	public void start() {
		logger.info("System start...");
		
		//init Persistence module
		Persistence.getInstance().start(vertx);
		
		MqttServerOptions options = new MqttServerOptions().setPort(50000);

		MqttServer mqttServer = MqttServer.create(vertx, options);
		mqttServer.endpointHandler(endpoint -> {

			initClient(endpoint);
			initClientConnection(endpoint);
			initClientSubscribe(endpoint);
			initClientPublish(endpoint);
		}).listen(ar -> {

			if (ar.succeeded()) {

				logger.info("MQTT server is listening on port[{}]", ar.result().actualPort());
			} else {

				logger.error("Error on starting the server, reason[{}]", ar.cause().getMessage());
				//ar.cause().printStackTrace();
			}
		});
		logger.info("System started");
	}
	
	@Override
	public void stop() {
		logger.info("System stopping...");
		Persistence.getInstance().stop();
		logger.info("System stopped");
	}

	private void initClient(MqttEndpoint endpoint) {
		Connect cmd = new Connect(endpoint);
		cmd.process();
	}

	private void initClientConnection(MqttEndpoint endpoint) {
		endpoint.disconnectHandler(v -> {
			logger.info("client [{}] send disconnect message", endpoint.clientIdentifier());
		});

		endpoint.closeHandler(v -> {
			CloseConnection cmd = new CloseConnection(endpoint);
			cmd.process();
		});
	}

	private void initClientSubscribe(MqttEndpoint endpoint) {
		endpoint.subscribeHandler(subscribe -> {
			Subscribe cmd = new Subscribe(endpoint, subscribe);
			cmd.process();
		});

		endpoint.unsubscribeHandler(unsubscribe -> {
			Unsubscribe cmd = new Unsubscribe(endpoint, unsubscribe);
			cmd.process();
		});
	}
	
	private void initClientPublish(MqttEndpoint endpoint) {
		endpoint.publishHandler(publish -> {
			Publish cmd = new Publish(endpoint, publish);
			cmd.process();
		});
		
		endpoint.publishAcknowledgeHandler(id -> {
			PubAck cmd = new PubAck(endpoint, id);
			cmd.process();
		});
	}
}
