package cn.lihua.modules.iot.javanative.dml;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.Before;
import org.junit.Test;

public class RecordInsertTest {
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
	
	/**
	 * 插入Record
	 * @throws IoTDBConnectionException
	 * @throws StatementExecutionException 
	 */
	@Test
	public void insertRecordTest() throws IoTDBConnectionException, StatementExecutionException {

		//session.setStorageGroup("root.group1");
		String deviceId = "root.group1.device1";
		List<String> measurements = new ArrayList<>();
		measurements.add("s0");
		measurements.add("s1");
		measurements.add("s2");
		List<TSDataType> types = new ArrayList<>();
		types.add(TSDataType.INT32);
		types.add(TSDataType.INT32);
		types.add(TSDataType.INT32);
		List<Object> values = new ArrayList<>();
		values.add(1);
		values.add(2);
		values.add(3);
		long timestamp = System.currentTimeMillis();
		session.insertRecord(deviceId, timestamp, measurements, types, values);

	}
	
}
