package com.zhangjie.mqtt;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.persist.Persistence;
import com.zhangjie.mqtt.persist.PersistenceCallback;
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
		Persistence.getInstance().stop();
	}

	private void initClient(MqttEndpoint endpoint) {
		// shows main connect info
		
		if (endpoint.auth() == null) {
			logger.info("MQTT client [{}] request to connect, clean session[{}], keepAliveTime[{}]",
					endpoint.clientIdentifier(), endpoint.isCleanSession(), endpoint.keepAliveTimeSeconds());
		} else {
			logger.info("MQTT client [{}] request to connect, clean session[{}], username[{}], password[{}], keepAliveTime[{}]",
					endpoint.clientIdentifier(), endpoint.isCleanSession(),
					endpoint.auth().userName(), endpoint.auth().password(), endpoint.keepAliveTimeSeconds());
		}
		/*if (endpoint.will() != null) {
			System.out.println("[will topic = " + endpoint.will().willTopic() + " msg = "
					+ endpoint.will().willMessage() + " QoS = " + endpoint.will().willQos() + " isRetain = "
					+ endpoint.will().isWillRetain() + "]");
		}*/

		Persistence.getInstance().saveClientConnection(endpoint.clientIdentifier(), "nodeId1",
				new PersistenceCallback() {
					@Override
					public void onSucceed(Integer insertId) {
						// accept connection from the remote client
						endpoint.accept(true);
						ClientManager.getInstance().addNewClient(endpoint.clientIdentifier(), endpoint);
					}

					@Override
					public void onFail(Throwable t) {
						logger.error("Failed to save client[{}] connection info, close client connection. reason[{}]",
								endpoint.clientIdentifier(), t.getMessage());
						endpoint.close();
					}
				});
	}

	private void initClientConnection(MqttEndpoint endpoint) {
		endpoint.disconnectHandler(v -> {
			logger.info("client [{}] send disconnect message", endpoint.clientIdentifier());
		});

		endpoint.closeHandler(v -> {
			logger.info("client [{}] closed connection", endpoint.clientIdentifier());
			ClientManager.getInstance().removeClient(endpoint.clientIdentifier());
		});
	}

	private void initClientSubscribe(MqttEndpoint endpoint) {
		endpoint.subscribeHandler(subscribe -> {

			ArrayList<MqttTopicQos> subscribeInfo = new ArrayList<>();
			List<MqttQoS> grantedQosLevels = new ArrayList<>();
			StringBuilder sb = new StringBuilder();
			for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {
				sb.append(s.topicName()).append(":").append(s.qualityOfService().value()).append(",");

				grantedQosLevels.add(s.qualityOfService());
				
				subscribeInfo.add(new MqttTopicQos(s.topicName(), s.qualityOfService().value()));
			}
			logger.info("Client[{}] subscribe info[{}]", endpoint.clientIdentifier(), sb.toString());
			
			Persistence.getInstance().saveClientSubscribe(endpoint.clientIdentifier(), subscribeInfo,
					new PersistenceCallback() {
						@Override
						public void onSucceed(Integer insertId) {
							// ack the subscriptions request
							endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
							SubscribeInfo.getInstance().addNewSubscribeInfos(endpoint.clientIdentifier(),
									subscribeInfo);
						}

						@Override
						public void onFail(Throwable t) {
							logger.error("Failed to save client[{}] subscribe info, close client connection. reason[{}]",
									endpoint.clientIdentifier(), t.getMessage());
							endpoint.close();
						}
					});
		});

		endpoint.unsubscribeHandler(unsubscribe -> {
			StringBuilder sb = new StringBuilder();
			for (String t : unsubscribe.topics()) {
				sb.append(t).append(",");
			}
			logger.info("client[{}] unsubscribe info[{}]", endpoint.clientIdentifier(), sb.toString());
			
			Persistence.getInstance().removeClientSubscribe(endpoint.clientIdentifier(), unsubscribe.topics(),
					new PersistenceCallback() {
						@Override
						public void onSucceed(Integer insertId) {
							// ack the subscriptions request
							endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
						}

						@Override
						public void onFail(Throwable t) {
							logger.error("Failed to remove client[{}] subscribe info, close client connection. reason[{}]",
									endpoint.clientIdentifier(), t.getMessage());
							endpoint.close();
						}
					});
		});
	}
	
	private void initClientPublish(MqttEndpoint endpoint) {
		endpoint.publishHandler(publish -> {
			String topic = publish.topicName();
			int publishQos = publish.qosLevel().value();
			
			logger.info("Receive Publish msg from client[{}], topic[{}], qos[{}], message[{}]",
					endpoint.clientIdentifier(), topic, publishQos, new String(publish.payload().getBytes()));
			
			List<ClientIdQos> subscribedClients = SubscribeInfo.getInstance().getSubscribedClients(topic);
			
			if (subscribedClients == null) {
				if (publishQos > 0) {
					endpoint.publishAcknowledge(publish.messageId());
				}
				logger.info("There is no subscribed client");
				return;
			}
			
			//check if need to save message into db.
			boolean needSaveMessage = false;
			if (publishQos > 0) {
				for (ClientIdQos ciq : subscribedClients) {
					if (ciq.getQos() > 0) {//TODO: still need to check 'clean session' flag
						needSaveMessage = true;
						break;
					}
				}
			}
			
			if (needSaveMessage) {
				Persistence.getInstance().saveMessage(endpoint.clientIdentifier(), topic,
						publish.messageId(), publish.payload().getBytes(), new PersistenceCallback() {
					@Override
					public void onSucceed(Integer insertId) {
						for (ClientIdQos ciq : subscribedClients) {
							int subscribedQos = ciq.getQos();
							if (publishQos < subscribedQos) {
								subscribedQos = publishQos;
							}
							final int qos = subscribedQos;
							
							Client client = ClientManager.getInstance().getClient(ciq.getClientId());
							if (client == null) {
								continue;
							}
							
							if (qos > 0) {//TODO: need to check 'clean session' flag
								//save client output message list
								Persistence.getInstance().saveClientMessage(client.endpoint().clientIdentifier(),
										insertId.intValue(), qos, new PersistenceCallback() {
									@Override
									public void onSucceed(Integer id) {
										logger.info("Save and send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
												client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
										client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
										int packetId = client.endpoint().lastMessageId();
										client.savePublishMessage(packetId, insertId.intValue(), topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
										logger.info("Save publish message, packetId[{}], insertId[{}]", packetId, insertId);
									}
									
									@Override
									public void onFail(Throwable t) {
										logger.error("Failed to save client[{}] message, reason[{}]",
												client.endpoint().clientIdentifier(), t.getMessage());
									}
								});
							} else {
								//send message to client
								logger.info("Send Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
										client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
								client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
							}
						}
					}

					@Override
					public void onFail(Throwable t) {
						logger.error("Failed to save client[{}] message, reason[{}]",
								endpoint.clientIdentifier(), t.getMessage());
					}
				});
			} else {
				//No need to save message
				for (ClientIdQos ciq : subscribedClients) {
					int subscribedQos = ciq.getQos();
					if (publishQos < subscribedQos) {
						subscribedQos = publishQos;
					}
					final int qos = subscribedQos;
					
					Client client = ClientManager.getInstance().getClient(ciq.getClientId());
					if (client != null) {
						logger.info("Send without save Publish msg to client[{}], topic[{}], qos[{}], message[{}]",
								client.endpoint().clientIdentifier(), topic, qos, new String(publish.payload().getBytes()));
						client.endpoint().publish(topic, publish.payload(), MqttQoS.valueOf(qos), false, false);
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
				logger.info("PubAck client[{}], packetId[{}], insertId[{}]",
						endpoint.clientIdentifier(), id, insertId);
				Persistence.getInstance().removeClientMessage(endpoint.clientIdentifier(), insertId,
						new PersistenceCallback() {

							@Override
							public void onSucceed(Integer insertId) {
							}

							@Override
							public void onFail(Throwable t) {
								logger.error("Failed to remove client[{}] message, reason[{}]",
										endpoint.clientIdentifier(), t.getMessage());
							}
				});
			}
		});
	}
}
