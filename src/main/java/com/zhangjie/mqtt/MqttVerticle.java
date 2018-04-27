package com.zhangjie.mqtt;

import java.util.ArrayList;
import java.util.List;

import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.persist.SaveInfoCallback;
import com.zhangjie.mqtt.subscribe.ClientIdQos;
import com.zhangjie.mqtt.subscribe.SubscribeInfo;
import com.zhangjie.mqtt.client.Client;
import com.zhangjie.mqtt.client.ClientManager;
import com.zhangjie.mqtt.persist.MqttTopicQos;

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
			initClientConnection(endpoint);
			initClientSubscribe(endpoint);
			initClientPublish(endpoint);
		}).listen(ar -> {

			if (ar.succeeded()) {

				System.out.println("MQTT server is listening on port " + ar.result().actualPort());
			} else {

				System.out.println("Error on starting the server");
				ar.cause().printStackTrace();
			}
		});
	}
	
	@Override
	public void stop() {
		Persistence.getInstance().stop();
	}

	private void initClient(MqttEndpoint endpoint) {
		// shows main connect info
		System.out.println("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = "
				+ endpoint.isCleanSession());

		if (endpoint.auth() != null) {
			System.out.println(
					"[username = " + endpoint.auth().userName() + ", password = " + endpoint.auth().password() + "]");
		}
		if (endpoint.will() != null) {
			System.out.println("[will topic = " + endpoint.will().willTopic() + " msg = "
					+ endpoint.will().willMessage() + " QoS = " + endpoint.will().willQos() + " isRetain = "
					+ endpoint.will().isWillRetain() + "]");
		}

		System.out.println("[keep alive timeout = " + endpoint.keepAliveTimeSeconds() + "]");

		Persistence.getInstance().saveClientConnection(endpoint.clientIdentifier(), "nodeId1",
				new SaveInfoCallback() {
					@Override
					public void onSucceed(Integer insertId) {
						// accept connection from the remote client
						endpoint.accept(true);
						ClientManager.getInstance().addNewClient(endpoint.clientIdentifier(), endpoint);
					}

					@Override
					public void onFail() {
						System.out.println("Failed to save connection info, close client connection");
						endpoint.close();
					}
				});
	}

	private void initClientConnection(MqttEndpoint endpoint) {
		endpoint.disconnectHandler(v -> {
			System.out.println("MQTT client [ " + endpoint.clientIdentifier() + "] send disconnect message");
			//endpoint.close();
			ClientManager.getInstance().removeClient(endpoint.clientIdentifier());
		});

		endpoint.closeHandler(v -> {
			System.out.println("MQTT client [ " + endpoint.clientIdentifier() + "] closed connection");
		});
	}

	private void initClientSubscribe(MqttEndpoint endpoint) {
		endpoint.subscribeHandler(subscribe -> {

			ArrayList<MqttTopicQos> subscribeInfo = new ArrayList<>();
			List<MqttQoS> grantedQosLevels = new ArrayList<>();
			for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {
				System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
				grantedQosLevels.add(s.qualityOfService());
				
				subscribeInfo.add(new MqttTopicQos(s.topicName(), s.qualityOfService().value()));
			}
			
			Persistence.getInstance().saveClientSubscribe(endpoint.clientIdentifier(), subscribeInfo,
					new SaveInfoCallback() {
						@Override
						public void onSucceed(Integer insertId) {
							// ack the subscriptions request
							endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
							SubscribeInfo.getInstance().addNewSubscribeInfos(endpoint.clientIdentifier(),
									subscribeInfo);
						}

						@Override
						public void onFail() {
							System.out.println("Failed to save subscribe info, close client connection");
							endpoint.close();
						}
					});
			


		});

		endpoint.unsubscribeHandler(unsubscribe -> {

			for (String t : unsubscribe.topics()) {
				System.out.println("Unsubscription for " + t);
			}
			
			Persistence.getInstance().removeClientSubscribe(endpoint.clientIdentifier(), unsubscribe.topics(),
					new SaveInfoCallback() {
						@Override
						public void onSucceed(Integer insertId) {
							System.out.println("remove subscribe ok");
							// ack the subscriptions request
							endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
						}

						@Override
						public void onFail() {
							System.out.println("Failed to remove subscribe info, close client connection");
							endpoint.close();
						}
					});
		});
	}
	
	private void initClientPublish(MqttEndpoint endpoint) {
		endpoint.publishHandler(publish -> {
			String topic = publish.topicName();
			int publishQos = publish.qosLevel().value();
			
			boolean savedMessage = false;
			
			List<ClientIdQos> subscribedClients = SubscribeInfo.getInstance().getSubscribedClients(topic);
			if (subscribedClients != null) {
				for (ClientIdQos ciq : subscribedClients) {
					int subscribedQos = ciq.getQos();
					if (publishQos < subscribedQos) {
						subscribedQos = publishQos;
					}
					final int qos = subscribedQos;
					
					Client client = ClientManager.getInstance().getClient(ciq.getClientId());
					if (client != null) {
						if (!savedMessage) {
							savedMessage = true;
							Persistence.getInstance().saveMessage(endpoint.clientIdentifier(), topic,
								publish.messageId(), publish.payload().getBytes(),
								new SaveInfoCallback() {
									@Override
									public void onSucceed(Integer insertId) {
										Persistence.getInstance().saveClientMessage(client.endpoint().clientIdentifier(), insertId.intValue(), qos,
												new SaveInfoCallback() {
											@Override
											public void onSucceed(Integer id) {
												client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
												int msgId = client.endpoint().lastMessageId();
												client.savePublishMessage(msgId, insertId.intValue(), topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
												System.out.println("Save publish message, msgId:" + msgId + ", insertId:" + insertId);
											}
											
											@Override
											public void onFail() {
												System.out.println("Failed to save client message");
											}
										});
									}

									@Override
									public void onFail() {
										System.out.println("Failed to save message");
									}
								});
						}
						
						
					}
				}
			}

			if (publishQos > 0) {
				endpoint.publishAcknowledge(publish.messageId());
			}
		});
		
		endpoint.publishAcknowledgeHandler(id -> {
			Client client = ClientManager.getInstance().getClient(endpoint.clientIdentifier());
			if (client != null) {
				int insertId = client.removePublishMessage(id);
				System.out.println("PubAck client:" + endpoint.clientIdentifier() + ", msgId:" + id + ", insertId:" + insertId);
				Persistence.getInstance().removeClientMessage(endpoint.clientIdentifier(), insertId,
						new SaveInfoCallback() {

							@Override
							public void onSucceed(Integer insertId) {
								System.out.println("Remove client msg ok");
							}

							@Override
							public void onFail() {
								System.out.println("Remove client msg failed");
							}
				});
			}
		});
	}
}
