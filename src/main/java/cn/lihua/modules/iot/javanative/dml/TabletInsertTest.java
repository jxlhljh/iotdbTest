package cn.lihua.modules.iot.javanative.dml;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Before;
import org.junit.Test;

public class TabletInsertTest {

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
	 * 插入Tablet
	 * @throws StatementExecutionException
	 * @throws IoTDBConnectionException
	 */
	@Test
	public void insertTabletTest() throws StatementExecutionException, IoTDBConnectionException {
		
	    int BATCH_INSERT_SIZE = 10000;
	    long TOTAL_INSERT_ROW_COUNT = 20003L;
		
		session.setFetchSize(2048);
		session.open(false);
		/*
		 * 一个Tablet例子:
		 * deviceID: root.ln.wf01.wt01
		 * time status, temperature, speed
		 * 1    true        1.0       1
		 * 2    false       2.0       2
		 * 3    true        3.0       3
		 */
		// 设置设备名字，设备下面的传感器名字，各个传感器的类型
		List<MeasurementSchema> schemaList = new ArrayList<>();
		schemaList.add(new MeasurementSchema("status", TSDataType.BOOLEAN));
		schemaList.add(new MeasurementSchema("temperature", TSDataType.DOUBLE));
		schemaList.add(new MeasurementSchema("speed", TSDataType.INT64));

		Tablet tablet = new Tablet("root.ln.wf01.wt02", schemaList, BATCH_INSERT_SIZE);


		// 以当前时间戳作为插入的起始时间戳
		long timestamp = System.currentTimeMillis();

		long row = 0;
		for (row = 0; row < TOTAL_INSERT_ROW_COUNT; row++) {
		    int rowIndex = tablet.rowSize++;
		    tablet.addTimestamp(rowIndex, timestamp);
		    // 随机生成数据
		    tablet.addValue("status", rowIndex, (row & 1) == 0);
		    tablet.addValue("temperature", rowIndex, (double) row);
		    tablet.addValue("speed", rowIndex, row);

		    if (tablet.rowSize == tablet.getMaxRowNumber()) {
		        session.insertTablet(tablet);
		        tablet.reset();
		        System.out.println("已经插入了：" + (row + 1) + "行数据");
		    }
		    timestamp++;
		}

		// 插入剩余不足 BATCH_INSERT_SIZE的数据
		if (tablet.rowSize != 0) {
		    session.insertTablet(tablet);
		    tablet.reset();
		    System.out.println("已经插入了：" + (row) + "行数据");
		}
		
	}
}
