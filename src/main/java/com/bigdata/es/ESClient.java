package com.bigdata.es;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESClient {
	private static byte[] LOCK = new byte[0];
	private static Client client = null;
	
	
	/**
	 * 读取配置文件
	 * @param propFile
	 * @return
	 */
	public static Properties readProperties(String propFile) {
        Properties props = new Properties();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(propFile)) {
            props.load(is);
        } catch (IOException e){
        	//logger.error(e);
        }
        return props;
    }

	/**
	 * 获取客户端
	 * 
	 * @return
	 */
	public static Client getEsClient() {
		synchronized (LOCK) {
			if (client == null) {
				Properties props = readProperties("app.properties");
				Settings settings = Settings.builder().put("client.transport.sniff", true).put("cluster.name", props.getProperty("es.cluster.name").trim()).build();
				try {
					String[] nodeHosts = props.getProperty("es.cluster.hosts").trim().split(",");
					PreBuiltTransportClient preClient = new PreBuiltTransportClient(settings);
					for (String nodeHost : nodeHosts){
						preClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeHost), Integer.valueOf(props.getProperty("es.cluster.port").trim())));
					}
					client = preClient;
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}
		return client;
	}
	
	/**
	 * 关闭
	 */
	public static void closeEsClient(){
		if (client != null){
			client.close();
		}
	}
}
