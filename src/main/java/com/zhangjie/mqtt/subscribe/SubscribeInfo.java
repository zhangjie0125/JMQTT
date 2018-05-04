package com.zhangjie.mqtt.subscribe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhangjie.mqtt.persist.MqttTopicQos;

class SubscribeTreeHelper {
	public static List<SubscribeTreeNode> getAllMatchNodes(
			HashMap<String/*topic filter*/, SubscribeTreeNode> map, String topicFilter) {
		List<SubscribeTreeNode> nodes = new ArrayList<>();
		
		SubscribeTreeNode treeNode = map.get(topicFilter);
		if (treeNode != null) {
			nodes.add(treeNode);
		}
		treeNode = map.get("+");
		if (treeNode != null) {
			nodes.add(treeNode);
		}
		treeNode = map.get("#");
		if (treeNode != null) {
			nodes.add(treeNode);
		}
		
		return nodes;
	}
}

class SubscribeTreeNode {
	private String topicFilter;
	private List<ClientIdQos> subscribedClients;
	private HashMap<String/*topic filter*/, SubscribeTreeNode> children;
	
	public SubscribeTreeNode(String topicFilter) {
		this.topicFilter = topicFilter;
		subscribedClients = new ArrayList<>();
		children = new HashMap<>();
	}
	
	public SubscribeTreeNode(String topicFilter, List<ClientIdQos> clients) {
		this.topicFilter = topicFilter;
		subscribedClients = clients;
		children = new HashMap<>();
	}

	public String getTopicFilter() {
		return topicFilter;
	}

	public List<ClientIdQos> getSubscribedClients() {
		return subscribedClients;
	}
	
	//return children topic filter '#' subscribed clients
	public List<ClientIdQos> getChildrenPoundSubscribedClients() {
		if (children.containsKey("#")) {
			return children.get("#").getSubscribedClients();
		} else {
			return new ArrayList<>();
		}
	}

	public void addSubscribedClient(String clientId, int qos) {
		boolean addNewClient = true;
		for (ClientIdQos cq : subscribedClients) {
			if (cq.getClientId() == clientId) {
				//exist client id, just update qos
				cq.setQos(qos);
				addNewClient = false;
				break;
			}
		}
		
		if (addNewClient) {
			ClientIdQos cq = new ClientIdQos(clientId, qos);
			subscribedClients.add(cq);
		}
	}

	public List<SubscribeTreeNode> findTopicFilter(String topicFilter) {
		List<SubscribeTreeNode> nodes = new ArrayList<>();
		
		if (this.topicFilter.equals("#")) {
			//should return this node
			nodes.add(this);
		} else {
			nodes.addAll(SubscribeTreeHelper.getAllMatchNodes(children, topicFilter));
		}
		
		return nodes;
	}
	
	public SubscribeTreeNode createOrFindTopicFilter(String topicFilter) {
		SubscribeTreeNode node = children.get(topicFilter);
		if (node == null) {
			node = new SubscribeTreeNode(topicFilter);
			children.put(topicFilter, node);
		}
		return node;
	}
}

class SubscribeTree {
	private HashMap<String/*topic filter*/, SubscribeTreeNode> roots;
	
	public SubscribeTree() {
		roots = new HashMap<>();
	}
	
	public void addSubscribeInfo(String clientId, String topic, int qos) throws SubscribeTreeException {
		String[] topicFilters = topic.split("/");
    	if (topicFilters.length == 0) {
    		throw new SubscribeTreeException("Invalid topic");
    	}
    	String root = topicFilters[0];
    	
    	//find if root topic filter exist
    	SubscribeTreeNode treeNode = roots.get(root);
    	if (treeNode == null) {
    		treeNode = new SubscribeTreeNode(root);
    		roots.put(root, treeNode);
    	}
    	
    	for (int i = 1; i < topicFilters.length; i++) {
    		String topicFilter = topicFilters[i];
    		
    		treeNode = treeNode.createOrFindTopicFilter(topicFilter);
    	}
    	
    	treeNode.addSubscribedClient(clientId, qos);
	}
	
	public List<ClientIdQos> getSubscribedClients(String topic) {
		String[] topicFilters = topic.split("/");
    	if (topicFilters.length == 0) {
    		return new ArrayList<>();
    	}
    	String root = topicFilters[0];
    	
    	//this function is ONLY called in PUBLISH message
    	//so no need to check topic wildcard in 'topicFilters'
    	
    	List<SubscribeTreeNode> matchNodes = new ArrayList<>();

		//check root topic filter
    	matchNodes.addAll(SubscribeTreeHelper.getAllMatchNodes(roots, root));

    	for (int i = 1; i < topicFilters.length; i++) {
    		String topicFilter = topicFilters[i];
    		
    		List<SubscribeTreeNode> tempMatchNodes = new ArrayList<>();
    		
    		for (SubscribeTreeNode node : matchNodes) {
        		List<SubscribeTreeNode> childrenNodes = node.findTopicFilter(topicFilter);
        		if (childrenNodes.isEmpty()) {
        			continue;
        		}
        		tempMatchNodes.addAll(childrenNodes);
    		}

    		matchNodes = tempMatchNodes;
    	}
    	
    	List<ClientIdQos> clients = new ArrayList<>();
    	for (SubscribeTreeNode node : matchNodes) {
    		clients.addAll(node.getSubscribedClients());
    		clients.addAll(node.getChildrenPoundSubscribedClients());
    	}
    	
    	return clients;
	}
}

class SubscribeTreeException extends Exception {

	private static final long serialVersionUID = -1390731895432811355L;

	public SubscribeTreeException() {
        super();
    }
 
    public SubscribeTreeException(String message) {
        super(message);
    }
 
    public SubscribeTreeException(String message, Throwable cause) {
        super(message, cause);
    }
 
    public SubscribeTreeException(Throwable cause) {
        super(cause);
    }
}

public class SubscribeInfo {
    private volatile static SubscribeInfo instance;
    private Lock lock;
    private SubscribeTree subscribeTree;
    
    private static final Logger logger = LoggerFactory.getLogger(SubscribeInfo.class);

    public static SubscribeInfo getInstance(){
        if(instance == null){
            synchronized (SubscribeInfo.class){
                if(instance == null){
                    instance = new SubscribeInfo();
                }
            }
        }
        return instance;
    }
    
    private SubscribeInfo() {
    	subscribeTree = new SubscribeTree();
    	lock = new ReentrantLock();
    }
    
    public void addNewSubscribeInfo(String clientId, String topic, int qos) {
    	logger.info("Add subscribe info, client[{}], topic[{}], qos[{}]", clientId, topic, qos);

    	try {
	    	lock.lock();
	    	subscribeTree.addSubscribeInfo(clientId, topic, qos);
    	} catch (SubscribeTreeException e) {
    		logger.error("addNewSubscribeInfo failed, reason[{}]", e.getMessage());
    	} finally {
    		lock.unlock();
    	}
    }
    
    public void addNewSubscribeInfos(String clientId, List<MqttTopicQos> info) {
    	try {
        	StringBuilder sb = new StringBuilder();
        	lock.lock();
        	
        	for (MqttTopicQos tq : info) {
        		sb.append(tq.getTopic()).append(":").append(tq.getQos()).append(",");
        		
        		subscribeTree.addSubscribeInfo(clientId, tq.getTopic(), tq.getQos());
        	}
        	logger.info("Add subscribe info, client[{}], topic-qos[{}]", clientId, sb.toString());
    	} catch (SubscribeTreeException e) {
    		logger.error("addNewSubscribeInfo failed, reason[{}]", e.getMessage());
    	} finally {
    		lock.unlock();
    	}
    }
    
    public List<ClientIdQos> getSubscribedClients(String topic) {
    	lock.lock();
    	List<ClientIdQos> subscribedClients = subscribeTree.getSubscribedClients(topic);
    	lock.unlock();
    	return subscribedClients;
    }
}
