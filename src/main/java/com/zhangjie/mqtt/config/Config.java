package com.zhangjie.mqtt.config;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.zhangjie.mqtt.cluster.Cluster;

public class Config {
	private volatile static Config instance;
	
	private String clusterHost;
	private String zkUrl;
	private String zkRootPath;
	
	private String persistenceType = "";
	
	private String mysqlHost;
	private String mysqlPort;
	private String mysqlUsername;
	private String mysqlPassword;
	private String mysqlDbname;
	private String mysqlMaxPoolSize;
	private String mysqlQueryTimeout;
	
    public static Config getInstance(){
        if(instance == null){
            synchronized (Cluster.class){
                if(instance == null){
                    instance = new Config();
                }
            }
        }
        return instance;
    }
    
    private Config() {
    }
    
    public void loadConfig(String filename) throws ConfigException {
		try {
			File f = new File(filename);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(f);
			NodeList nl = doc.getElementsByTagName("cluster");
			if (nl.getLength() > 1) {
				throw new ConfigException("There are more than 1 'cluster' block");
			}
			readClusterConfig(nl.item(0));
			
			nl = doc.getElementsByTagName("persistence");
			if (nl.getLength() > 1) {
				throw new ConfigException("There are more than 1 'persistence' block");
			}
			readPersistenceConfig(nl.item(0));
		} catch (Exception e) {
			throw new ConfigException("failed to read config file", e);
		}
    }
    
    private void readClusterConfig(Node node) {
    	NodeList childNodes = node.getChildNodes();
    	for (int i = 0; i < childNodes.getLength(); i++) {
    		if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE) {
    			if (childNodes.item(i).getNodeName().equals("host")) {
    				clusterHost = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("zookeeper")) {
    				NodeList zkChildNodes = childNodes.item(i).getChildNodes();
    				for (int j = 0; j < zkChildNodes.getLength(); j++) {
    					if (zkChildNodes.item(j).getNodeType() == Node.ELEMENT_NODE) {
    						if (zkChildNodes.item(j).getNodeName().equals("url")) {
    							zkUrl = zkChildNodes.item(j).getFirstChild().getNodeValue();
    						} else if (zkChildNodes.item(j).getNodeName().equals("root_path")) {
    							zkRootPath = zkChildNodes.item(j).getFirstChild().getNodeValue();
    						}
    					}
    				}
    			}
    		}
    	}
    }
    
    private void readPersistenceConfig(Node node) {
    	NodeList childNodes = node.getChildNodes();
    	for (int i = 0; i < childNodes.getLength(); i++) {
    		if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE) {
    			if (childNodes.item(i).getNodeName().equals("mysql")) {
    				persistenceType = "mysql";
    				readMysqlConfig(childNodes.item(i));
    			}
    		}
    	}
    }
    
    private void readMysqlConfig(Node node) {
    	NodeList childNodes = node.getChildNodes();
    	for (int i = 0; i < childNodes.getLength(); i++) {
    		if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE) {
    			if (childNodes.item(i).getNodeName().equals("host")) {
    				mysqlHost = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("port")) {
    				mysqlPort = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("username")) {
    				mysqlUsername = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("password")) {
    				mysqlPassword = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("database")) {
    				mysqlDbname = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("max_pool_size")) {
    				mysqlMaxPoolSize = childNodes.item(i).getFirstChild().getNodeValue();
    			} else if (childNodes.item(i).getNodeName().equals("query_timeout")) {
    				mysqlQueryTimeout = childNodes.item(i).getFirstChild().getNodeValue();
    			}
    		}
    	}
    }

	public String getClusterHost() {
		return clusterHost;
	}

	public String getZkUrl() {
		return zkUrl;
	}

	public String getZkRootPath() {
		return zkRootPath;
	}

	public String getPersistenceType() {
		return persistenceType;
	}

	public String getMysqlHost() {
		return mysqlHost;
	}

	public String getMysqlPort() {
		return mysqlPort;
	}

	public String getMysqlUsername() {
		return mysqlUsername;
	}

	public String getMysqlPassword() {
		return mysqlPassword;
	}

	public String getMysqlDbname() {
		return mysqlDbname;
	}

	public String getMysqlMaxPoolSize() {
		return mysqlMaxPoolSize;
	}

	public String getMysqlQueryTimeout() {
		return mysqlQueryTimeout;
	}
}
