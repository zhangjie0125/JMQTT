package com.zhangjie.mqtt.subscribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSubscribeInfo {
	
	@Before
    public void setUp() throws Exception {
    }
	
	@After
    public void tearDown() throws Exception {
    }
	
	@Test
	public void testRootSubscribe() {
		SubscribeInfo info = SubscribeInfo.getInstance();
		info.addNewSubscribeInfo("clientId1", "sport", 0);
		List<ClientIdQos> clients = info.getSubscribedClients("sport");
		assertEquals(1, clients.size());
		ClientIdQos cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		info.addNewSubscribeInfo("clientId1", "sport", 1);
		clients = info.getSubscribedClients("sport");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		
		info.addNewSubscribeInfo("clientId2", "sport", 0);
		clients = info.getSubscribedClients("sport");
		assertEquals(2, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		cq = clients.get(1);
		assertEquals("clientId2", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		info.addNewSubscribeInfo("clientId1", "sport1", 0);
		clients = info.getSubscribedClients("sport");
		assertEquals(2, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		cq = clients.get(1);
		assertEquals("clientId2", cq.getClientId());
		assertEquals(0, cq.getQos());
		clients = info.getSubscribedClients("sport1");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(0, cq.getQos());
	}

	@Test
	public void testNormalSubscribe() {
		SubscribeInfo info = SubscribeInfo.getInstance();
		info.addNewSubscribeInfo("clientId1", "sport/tennis/player1", 0);
		List<ClientIdQos> clients = info.getSubscribedClients("sport/tennis/player1");
		assertEquals(1, clients.size());
		ClientIdQos cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		info.addNewSubscribeInfo("clientId1", "sport/tennis/player1", 1);
		clients = info.getSubscribedClients("sport/tennis/player1");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		
		info.addNewSubscribeInfo("clientId2", "sport/tennis/player1", 0);
		clients = info.getSubscribedClients("sport/tennis/player1");
		assertEquals(2, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		cq = clients.get(1);
		assertEquals("clientId2", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		info.addNewSubscribeInfo("clientId1", "sport/tennis/player2", 0);
		clients = info.getSubscribedClients("sport/tennis/player1");
		assertEquals(2, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(1, cq.getQos());
		cq = clients.get(1);
		assertEquals("clientId2", cq.getClientId());
		assertEquals(0, cq.getQos());
		clients = info.getSubscribedClients("sport/tennis/player2");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(0, cq.getQos());
	}
	
	@Test
	public void testWildcardSubscribe() {
		SubscribeInfo info = SubscribeInfo.getInstance();
		info.addNewSubscribeInfo("clientId1", "sport/pingpang/+", 0);
		List<ClientIdQos> clients = info.getSubscribedClients("sport/pingpang/player1");
		assertEquals(1, clients.size());
		ClientIdQos cq = clients.get(0);
		assertEquals("clientId1", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		info.addNewSubscribeInfo("clientId2", "sport/+/player1", 0);
		clients = info.getSubscribedClients("sport/pingpang/player1");
		assertEquals(2, clients.size());
		boolean checkClientId1 = false;
		boolean checkClientId2 = false;
		for (ClientIdQos e : clients) {
			if (e.getClientId().equals("clientId1")) {
				assertEquals(0, e.getQos());
				checkClientId1 = true;
			} else if (e.getClientId().equals("clientId2")) {
				assertEquals(0, e.getQos());
				checkClientId2 = true;
			}
		}
		assertTrue(checkClientId1 && checkClientId2);
		
		info.addNewSubscribeInfo("clientId3", "sport/pingpang/#", 0);
		clients = info.getSubscribedClients("sport/pingpang/player1");
		assertEquals(3, clients.size());
		checkClientId1 = false;
		checkClientId2 = false;
		boolean checkClientId3 = false;
		for (ClientIdQos e : clients) {
			if (e.getClientId().equals("clientId1")) {
				assertEquals(0, e.getQos());
				checkClientId1 = true;
			} else if (e.getClientId().equals("clientId2")) {
				assertEquals(0, e.getQos());
				checkClientId2 = true;
			} else if (e.getClientId().equals("clientId3")) {
				assertEquals(0, e.getQos());
				checkClientId3 = true;
			}
		}
		assertTrue(checkClientId1 && checkClientId2 && checkClientId3);
		
		clients = info.getSubscribedClients("sport/pingpang/player1/score");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId3", cq.getClientId());
		assertEquals(0, cq.getQos());
		
		clients = info.getSubscribedClients("sport/pingpang");
		assertEquals(1, clients.size());
		cq = clients.get(0);
		assertEquals("clientId3", cq.getClientId());
		assertEquals(0, cq.getQos());
	}
}
