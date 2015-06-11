package org.opencloudb.route.function;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.rule.RuleAlgorithm;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import demo.catlets.BatchInsertSequence;

/**
 * 根据MpId分片，请求bas获取
 * 
 * @author yirongyi
 * 
 */
public class PartitionByMpId extends AbstractPartitionAlgorithm implements
		RuleAlgorithm {
	
	private static final Logger LOGGER = Logger.getLogger(PartitionByMpId.class);

	private Map<Integer, Integer> map;

	@Override
	public void init() {
//		initFromBAS();

	}

	private void initFromBAS() {
		try {
			URL url = new URL("http://127.0.0.1:8080/bas/api/db.do?method=mpIdShardRule");
			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();

			urlConnection.setRequestMethod("GET");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);
			urlConnection.setUseCaches(false);
			
			InputStream in = urlConnection.getInputStream();  
            JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<Integer, Integer>> typeRef = new TypeReference<HashMap<Integer, Integer>>() {};

			map = mapper.readValue(in, typeRef);
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+map);
		} catch (Exception e) {
//			e.printStackTrace();
			LOGGER.error("init PartitionByMpId error",e);
		}
	}
	
	@Override
	public Integer calculate(String columnValue) {
		if (map == null) {
			initFromBAS();
		}
		Integer mpId = Integer.valueOf(columnValue);
		Integer nodeIndex = map.get(mpId);
		return (nodeIndex == null||nodeIndex <=0)?0:nodeIndex;
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this,
				beginValue, endValue);
	}

}
