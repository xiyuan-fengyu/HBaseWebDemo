package com.xiyuan.hbase.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class TestController {
	
	@RequestMapping(value="/test")
	@ResponseBody
	public Map<String, Object> test() {
		Map<String, Object> result = new HashMap<String, Object>();
		
		result.put("success", true);
		result.put("message", "test");
		
		return result;
	}
	
	@RequestMapping(value="/test/hbase")
	@ResponseBody
	public Map<String, Object> testHbase() {
		Map<String, Object> result = new HashMap<String, Object>();
		ArrayList<String> list = new ArrayList<String>();
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");  
		conf.set("hbase.zookeeper.quorum", "192.168.1.240");
		conf.set("hbase.master", "192.168.1.240:6000"); 
		
		Connection connection = null;
		Table hbaseTbl = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			hbaseTbl = connection.getTable(TableName.valueOf("hbaseTest"));

			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("row0"));
			scan.setStopRow(Bytes.toBytes("row5"));
			ResultScanner resultScanner = hbaseTbl.getScanner(scan);
			Result next = resultScanner.next();
			while (next != null) {
				List<Cell> cells = next.listCells();
				for (Cell cell : cells) {
					list.add(Bytes.toString(CellUtil.cloneQualifier(cell)) + " = " + Bytes.toString(CellUtil.cloneValue(cell)));
				}
				next = resultScanner.next();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (hbaseTbl != null) {
					hbaseTbl.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		result.put("list", list);
		result.put("success", true);
		result.put("message", "test");
		
		return result;
	}
	
}
