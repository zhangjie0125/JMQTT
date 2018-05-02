package com.zhangjie.mqtt.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.client.ClientManager;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

public class MysqlPersistence implements MqttPersistence {
	private SQLClient client;
	
	private static final Logger logger = LoggerFactory.getLogger(MysqlPersistence.class);
	
	@Override
	public void start(Vertx vertx) {
		JsonObject mySQLClientConfig = new JsonObject()
				.put("host", "10.10.10.10")
				.put("port", 3306)
				.put("username", "mqttuser")
				.put("password", "123456")
				.put("database", "mqtt")
				.put("maxPoolSize", 100)
				.put("queryTimeout", 1000);
		client = MySQLClient.createShared(vertx, mySQLClientConfig);
	}

	@Override
	public void stop() {
		client.close();
	}
	
	@Override
	public void saveClientConnection(String clientId, String nodeId, PersistenceCallback cb) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(nodeId);
				conn.updateWithParams("INSERT INTO connection (client_id, node_id, time) VALUES( ?, ?, NOW() )",
						params, r -> {
							//ATTENTION: If a exception is throw in this function,
							//this function would be called again with 'r.succeeded() = false' 
							conn.close();
							if (r.succeeded()) {
								cb.onSucceed(0);
							} else {
								cb.onFail(r.cause());
							}
				});
			} else {
				logger.error("saveClientConnection getConnection failed[{}]", res.cause().getMessage());
			}
		});
		
	}

	@Override
	public void saveClientSubscribe(String clientId, ArrayList<MqttTopicQos> subscribeInfo, PersistenceCallback cb) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				StringBuilder sb = new StringBuilder("INSERT INTO subscribe (client_id, topic, qos, time) VALUES");
				boolean first = true;
				for (MqttTopicQos info : subscribeInfo) {
					if (!first) {
						sb.append(",");
					} else {
						first = false;
					}
					
					sb.append("('");
					sb.append(clientId);
					sb.append("','");
					sb.append(info.getTopic());
					sb.append("',");
					sb.append(info.getQos());
					sb.append(",NOW(6))");
				}
				sb.append(" ON DUPLICATE KEY UPDATE qos=VALUES(qos), time=NOW(6)");
				
				SQLConnection conn = res.result();
				conn.update(sb.toString(),
						r -> {
							conn.close();
							if (r.succeeded()) {
								cb.onSucceed(0);
							} else {
								cb.onFail(r.cause());
							}
				});
			} else {
				logger.error("saveClientSubscribe getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}

	@Override
	public void removeClientSubscribe(String clientId, List<String> topics, PersistenceCallback cb) {
		OperationCounterCallback occb = new OperationCounterCallback(topics.size());
		
		for (String topic : topics) {
			client.getConnection(res -> {
				if (res.succeeded()) {
					SQLConnection conn = res.result();
					JsonArray params = new JsonArray().add(clientId).add(topic);
					conn.updateWithParams("DELETE FROM subscribe WHERE client_id = ? AND topic = ?",
							params, r -> {
								conn.close();
								if (!r.succeeded()) {
									occb.setFailure();
								}
								if (occb.tick()) {
									if (occb.getFailureCount() == 0) {
										cb.onSucceed(0);
									} else {
										cb.onFail(r.cause());
									}
								}
					});
				} else {
					logger.error("removeClientSubscribe getConnection failed[{}]", res.cause().getMessage());
				}
			});
		}
	}
	
	@Override
	public void saveMessage(String clientId, String topic, int packetId, byte[] msg, PersistenceCallback cb) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(topic).add(msg).add(packetId).add(clientId);
				conn.updateWithParams("INSERT INTO message (topic, message, packet_id, client_id, time) VALUES( ?, ?, ?, ?, NOW(6) )",
						params, r -> {
							conn.close();
							if (r.succeeded()) {
								cb.onSucceed(r.result().getKeys().getInteger(0));
							} else {
								cb.onFail(r.cause());
							}
				});
			} else {
				logger.error("saveMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
	
	@Override
	public void saveClientMessage(String clientId, int msgId, int qos, PersistenceCallback cb) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(msgId).add(qos);
				conn.updateWithParams("INSERT INTO msg_list (client_id, message_id, qos, time) VALUES( ?, ?, ?, NOW(6) )",
						params, r -> {
							conn.close();
							if (r.succeeded()) {
								cb.onSucceed(0);
							} else {
								cb.onFail(r.cause());
							}
				});
			} else {
				logger.error("saveClientMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
	
	@Override
	public void removeClientMessage(String clientId, int msgId, PersistenceCallback cb) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(msgId);
				conn.updateWithParams("DELETE FROM msg_list WHERE client_id = ? AND message_id = ?",
						params, r -> {
							conn.close();
							if (r.succeeded()) {
								cb.onSucceed(0);
							} else {
								cb.onFail(r.cause());
							}
				});
			} else {
				logger.error("removeClientMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
}

class OperationCounterCallback {
	private AtomicInteger counter;
	private int total;
	private int fail;
	
	public OperationCounterCallback(int total) {
		this.total = total;
		fail = 0;
	}
	
	public boolean tick() {
		int value = counter.incrementAndGet();
		if (value >= total) {
			return true;
		} else {
			return false;
		}
	}
	
	public void setFailure() {
		fail++;
	}
	
	public int getFailureCount() {
		return fail;
	}
}
