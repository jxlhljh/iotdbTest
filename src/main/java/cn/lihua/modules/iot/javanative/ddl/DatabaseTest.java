package cn.lihua.modules.iot.javanative.ddl;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 数据库管理测试
 */
public class DatabaseTest {
	
	private String host = "192.168.56.101";
	private int port = 6667;
	private String user = "root";
	private String pwd = "123456";
	private Session session = null;
	
	@Before
	public void before() {
		session = 
			    new Session.Builder()
			        .host(host)
			        .port(port)
			        .username(user)
			        .password(pwd)
			        .build();
			
		try {
			session.open();
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		}
		
	}
	
	@After
	public void after() {
		try {
			session.close();
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建数据库
	 * @throws StatementExecutionException 
	 * @throws IoTDBConnectionException 
	 */
	@Test
	public void setStorageGroupTest() throws IoTDBConnectionException, StatementExecutionException {
		
		String storageGroupId = "root.group1";
		session.setStorageGroup(storageGroupId);
		
	}
	
	/**
	 * 删除数据库
	 * @throws StatementExecutionException 
	 * @throws IoTDBConnectionException 
	 */
	@Test
	public void deleteStorageGroupTest() throws IoTDBConnectionException, StatementExecutionException  {
		String storageGroupId = "root.group1";
		session.deleteStorageGroup(storageGroupId);
	}
	
	/**
	 * 删除多个
	 * @throws IoTDBConnectionException
	 * @throws StatementExecutionException
	 */
	@Test
	public void deleteStorageGroupsTest() throws IoTDBConnectionException, StatementExecutionException  {
		List<String> storageGroupIds = new ArrayList<String>();
		storageGroupIds.add("root.group1");
		session.deleteStorageGroups(storageGroupIds);
	}

}
