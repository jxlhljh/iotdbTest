package cn.lihua.modules.iot.javanative.dml;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeleteDataTest {
	
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
	 * 删除数据测试,
	 * 参数timestamp为long类型，表示要删除的数据的时间，在这时间或这时间之前的数据将删除
	 * Delete data before or equal to a timestamp of one or several timeseries
	 * @throws StatementExecutionException 
	 */
	@Test
	public void deleteDataTest() throws StatementExecutionException {
		
		String deviceId = "root.group1.device2.s0";
		long timestamp = System.currentTimeMillis();
		System.out.println(timestamp);
		try {
			session.deleteData(deviceId, timestamp);
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 批量删除数据测试,
	 * 参数timestamp为long类型，表示要删除的数据的时间，在这时间或这时间之前的数据将删除
	 * Delete data before or equal to a timestamp of one or several timeseries
	 * @throws StatementExecutionException
	 */
	@Test
	public void deleteDatasTest() throws StatementExecutionException {
		
		List<String> paths = new ArrayList<String>();
		paths.add("root.group1.device2.s0");
		paths.add("root.group1.device2.s1");
		paths.add("root.group1.device2.s2");
		
		//删除当前时间以前的数据
		long timestamp = System.currentTimeMillis();
		
		try {
			session.deleteData(paths, timestamp);
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		}
		
	}

}
