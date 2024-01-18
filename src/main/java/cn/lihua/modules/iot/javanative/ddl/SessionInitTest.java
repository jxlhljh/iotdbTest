package cn.lihua.modules.iot.javanative.ddl;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.junit.Test;

/**
 * Session初始化测试
 */
public class SessionInitTest {
	
	private String host = "192.168.56.101";
	private int port = 6667;
	
	/**
	 * 一般不使用，采用本机localhost和6667端口及默认账号密码root/root初始化Session
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void test01() throws IoTDBConnectionException {
		
		// use default configuration 
		Session session = new Session.Builder().build();
		System.out.println(session);
		session.open();
		session.close();
		
	}
	
	/**
	 * 通过Session.Builder构造器初始化Session,采用默认的账号root和默认密码root,指定IP和端口
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void test02() throws IoTDBConnectionException {
		
		// initialize with a single node
		Session session = 
		    new Session.Builder()
		        .host(host)
		        .port(port)
		        .build();
		
		session.open();
		System.out.println(session);
		session.close();
		
	}
	
	/**
	 * 采用自定义的账号和密码码初始化Session
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void test03() throws IoTDBConnectionException {
		
		Session session = 
			    new Session.Builder()
			        .host(host)
			        .port(port)
			        .username("root")
			        .password("123456")
			        .build();
		
		session.open();
		System.out.println(session);
		session.close();
		
	}
	
	/**
	 * 有多个节点的情况下，通过Session.Builder构造器初始化Session
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void test04() throws IoTDBConnectionException {
		
		// initialize with multiple nodes
		List<String> nodeUrls = new ArrayList<>();
		nodeUrls.add("192.168.56.101:6667");
		nodeUrls.add("192.168.56.102:6667");
		nodeUrls.add("192.168.56.103:6667");
		
		Session session = 
		    new Session.Builder()
		    	.nodeUrls(nodeUrls)
		        .username("root")
		        .password("123456")
		        .build();
		
		session.open();
		System.out.println(session);
		session.close();
		
	}
	

}
