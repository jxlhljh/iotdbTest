package cn.lihua.modules.iot.javanative.dml;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 查询测试
 */
public class QueryTest {

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
	 * 查询数据
	 */
	@Test
	public void executeRawDataQueryTest() throws StatementExecutionException, IoTDBConnectionException {
		
		List<String> paths = new ArrayList<>();
		paths.add("root.ln.wf01.wt01.temperature");
		paths.add("root.ln.wf01.wt01.speed");
		paths.add("root.ln.wf01.wt01.status");
		
		String startDateTimeStr = "2024-01-01 00:00:00";  
		LocalDateTime ldt = LocalDateTime.parse(startDateTimeStr,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		Date date = Date.from(ldt.atZone( ZoneId.systemDefault()).toInstant());
		long startTime = date.getTime();
		
		//当前时间
		long endTime = System.currentTimeMillis();
		
		SessionDataSet sessionDataSet = session.executeRawDataQuery(paths, startTime, endTime);
		
		while(sessionDataSet.hasNext()) {
			
			RowRecord rowRecord = sessionDataSet.next();
			System.out.println(rowRecord);
			
		}
		
	}
	
	/**
	 * 查询最新数据
	 * 查询指定时间以后的最新数据
	 * @throws StatementExecutionException
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void executeLastDataQueryTest() throws StatementExecutionException, IoTDBConnectionException {
		
		List<String> paths = new ArrayList<>();
		paths.add("root.ln.wf01.wt01.temperature");
		paths.add("root.ln.wf01.wt01.speed");
		paths.add("root.ln.wf01.wt01.status");
		
		//查询指定时间以后的最新数据，这里指定时间为"2024-01-01 00:00:00";
		String startDateTimeStr = "2024-01-01 00:00:00";  
		LocalDateTime ldt = LocalDateTime.parse(startDateTimeStr,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		Date date = Date.from(ldt.atZone( ZoneId.systemDefault()).toInstant());
		long startTime = date.getTime();
		
		SessionDataSet sessionDataSet = session.executeLastDataQuery(paths, startTime);
		
		while(sessionDataSet.hasNext()) {
			RowRecord rowRecord = sessionDataSet.next();
			System.out.println(rowRecord);
		}
		
	}
	
	/**
	 * IoTDB-SQL Interface
	 * Sql语句接口，
	 * @throws StatementExecutionException
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void executeQueryStatement() throws StatementExecutionException, IoTDBConnectionException {
		
		SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.ln.wf01.wt03");
		while(sessionDataSet.hasNext()) {
			RowRecord rowRecord = sessionDataSet.next();
			System.out.println(rowRecord);
		}
	}
	
	/**
	 * IoTDB-SQL Interface
	 * Sql语句更新接口
	 * @throws StatementExecutionException
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void executeNonQueryStatement() throws StatementExecutionException, IoTDBConnectionException {
		session.executeNonQueryStatement("INSERT INTO root.ln.wf01.wt03(timestamp,status) values(200,true);");
	}

	
	
}
