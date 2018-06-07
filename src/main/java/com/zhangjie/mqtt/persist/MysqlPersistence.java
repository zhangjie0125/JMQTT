package com.zhangjie.mqtt.persist;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.config.Config;

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
				.put("host", Config.getInstance().getMysqlHost())
				.put("port", Integer.parseInt(Config.getInstance().getMysqlPort()))
				.put("username", Config.getInstance().getMysqlUsername())
				.put("password", Config.getInstance().getMysqlPassword())
				.put("database", Config.getInstance().getMysqlDbname())
				.put("maxPoolSize", Integer.parseInt(Config.getInstance().getMysqlMaxPoolSize()))
				.put("queryTimeout", Integer.parseInt(Config.getInstance().getMysqlQueryTimeout()));
		client = MySQLClient.createShared(vertx, mySQLClientConfig);
	}

	@Override
	public void stop() {
		client.close();
	}
	
	@Override
	public void saveClientConnection(String clientId, String nodeId, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(nodeId);
				conn.updateWithParams("INSERT INTO connection (client_id, node_id, time) VALUES( ?, ?, NOW() )",
						params, r -> {
							//ATTENTION: If a exception is throw in this function,
							//this function would be called again with 'r.succeeded() = false' 
							conn.close();
							
							MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
							
							if (r.succeeded()) {
								result.setResult(r.result().getKeys().getInteger(0));
							} else {
								result.setCause(r.cause());
							}
							
							handler.handle(result);
				});
			} else {
				logger.error("saveClientConnection getConnection failed[{}]", res.cause().getMessage());
			}
		});
		
	}
	
	@Override
	public void getClientConnection(String clientId, MqttPersistenceHandler<MqttPersistenceResult<ClientConnectionInfo>> handler) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId);
				conn.queryWithParams("SELECT * FROM connection WHERE client_id = ? ORDER BY time DESC", params,
						r -> {
							conn.close();
							
							MqttPersistenceResult<ClientConnectionInfo> result = new MqttPersistenceResult<ClientConnectionInfo>();
							if (r.succeeded()) {
								if (r.result().getNumRows() == 0) {
									//this is the first time that client connects to me
									result.setResult((new ClientConnectionInfo("", 0)));
								} else {
									String nodeId = r.result().getRows().get(0).getString("node_id");
									long time = 0;
									SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
									String strTime = r.result().getRows().get(0).getString("time");
									try {
							            Date date=f.parse(strTime);
							            time = date.getTime();
							        } catch(ParseException px) {
							            logger.error("failed to parse {} to Date object", strTime);
							        }
									
									logger.info("connect time:{}", time);
									result.setResult((new ClientConnectionInfo(nodeId, time)));
								}
							} else {
								logger.error("failed to get client[{}] connection info, reason:{}",
										clientId, r.cause().getMessage());
								result.setCause(r.cause());
							}
							
							handler.handle(result);
						});
			} else {
				logger.error("saveClientConnection getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}

	@Override
	public void saveClientSubscribe(String clientId, ArrayList<MqttTopicQos> subscribeInfo, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
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
							
							MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
							if (r.succeeded()) {
								result.setResult(r.result().getKeys().getInteger(0));
							} else {
								result.setCause(r.cause());
							}
							handler.handle(result);
				});
			} else {
				logger.error("saveClientSubscribe getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}

	@Override
	public void removeClientSubscribe(String clientId, List<String> topics, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
		MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
		AtomicInteger counter = new AtomicInteger(0);
		int total = topics.size();
		
		for (String topic : topics) {
			client.getConnection(res -> {
				if (res.succeeded()) {
					SQLConnection conn = res.result();
					JsonArray params = new JsonArray().add(clientId).add(topic);
					conn.updateWithParams("DELETE FROM subscribe WHERE client_id = ? AND topic = ?",
							params, r -> {
								conn.close();
								
								if (!r.succeeded()) {
									//Error
									result.setCause(r.cause());
								}
								
								int value = counter.incrementAndGet();
								if (value >= total) {
									//all topics are removed
									result.setResult(value);
									handler.handle(result);
								}
					});
				} else {
					logger.error("removeClientSubscribe getConnection failed[{}]", res.cause().getMessage());
				}
			});
		}
	}
	
	@Override
	public void saveMessage(String clientId, String topic, int packetId, byte[] msg, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(topic).add(msg).add(packetId).add(clientId);
				conn.updateWithParams("INSERT INTO message (topic, message, packet_id, client_id, time) VALUES( ?, ?, ?, ?, NOW(6) )",
						params, r -> {
							conn.close();
							
							MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
							if (r.succeeded()) {
								result.setResult(r.result().getKeys().getInteger(0));
							} else {
								result.setCause(r.cause());
							}
							handler.handle(result);
				});
			} else {
				logger.error("saveMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
	
	@Override
	public void saveClientMessage(String clientId, int msgId, int qos, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(msgId).add(qos);
				conn.updateWithParams("INSERT INTO msg_list (client_id, message_id, qos, time) VALUES( ?, ?, ?, NOW(6) )",
						params, r -> {
							conn.close();
							
							MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
							if (r.succeeded()) {
								result.setResult(r.result().getKeys().getInteger(0));
							} else {
								result.setCause(r.cause());
							}
							handler.handle(result);
				});
			} else {
				logger.error("saveClientMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
	
	@Override
	public void removeClientMessage(String clientId, int msgId, MqttPersistenceHandler<MqttPersistenceResult<Integer>> handler) {
		client.getConnection(res -> {
			if (res.succeeded()) {
				SQLConnection conn = res.result();
				JsonArray params = new JsonArray().add(clientId).add(msgId);
				conn.updateWithParams("DELETE FROM msg_list WHERE client_id = ? AND message_id = ?",
						params, r -> {
							conn.close();
							
							MqttPersistenceResult<Integer> result = new MqttPersistenceResult<Integer>();
							if (r.succeeded()) {
								result.setResult(0);
							} else {
								result.setCause(r.cause());
							}
							handler.handle(result);
				});
			} else {
				logger.error("removeClientMessage getConnection failed[{}]", res.cause().getMessage());
			}
		});
	}
}
