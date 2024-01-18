package cn.lihua.modules.iot.javanative.ddl;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimeseriesTest {
	
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
	 * 创建Timeseries
	 */
	@Test
	public void createTimeseriesTest() {
		
        try {
			session.createTimeseries("root.sestest.wf01.wt01.s0", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);
	        session.createTimeseries("root.sestest.wf01.wt01.s1", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			e.printStackTrace();
		}
        
	}
	
	/**
	 * 创建多个Timeseries
	 */
	@Test
	public void createMultiTimeseriesTest() {
		
		List<String> paths = new ArrayList<String>();
		paths.add("root.sestest.wf01.wt01.s0");
		paths.add("root.sestest.wf01.wt01.s1");
		
		List<TSDataType> types = new ArrayList<>();
		types.add(TSDataType.INT32);
		types.add(TSDataType.INT32);

		List<TSEncoding> encodings = new ArrayList<>();
		encodings.add(TSEncoding.RLE);
		encodings.add(TSEncoding.RLE);

		List<CompressionType> compressionTypes = new ArrayList<>();
		compressionTypes.add(CompressionType.SNAPPY); // or null (default)
		compressionTypes.add(CompressionType.SNAPPY);

		try {
			session.createMultiTimeseries(paths,types, encodings, compressionTypes,null,null,null,null);
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 删除Timeseries
	 */
	@Test
	public void deleteTimeseriesTest() {

		try {
			
			//可以传一个timeseries
			//session.deleteTimeseries("root.sestest.wf01.wt01.s0");
			//session.deleteTimeseries("root.sestest.wf01.wt01.s1");
			
			//也可以传多个timeseries
			List<String> timeseries = new ArrayList<String>();
			timeseries.add("root.sestest.wf01.wt01.s0");
			timeseries.add("root.sestest.wf01.wt01.s1");
			session.deleteTimeseries(timeseries);
			
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 检查是否存在
	 */
	@Test
	public void checkTimeseriesExistsTest() {
		String timeseries = "root.sestest.wf01.wt01.s0";
		try {
			System.out.println(session.checkTimeseriesExists(timeseries));
		} catch (IoTDBConnectionException e) {
			e.printStackTrace();
		} catch (StatementExecutionException e) {
			e.printStackTrace();
		}
	}


}
