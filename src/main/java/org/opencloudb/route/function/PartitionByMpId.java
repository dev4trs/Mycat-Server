package org.opencloudb.route.function;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.opencloudb.cache.CacheService;
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

	private final static String MPID_PROPERTIES = "/bas.properties";
	private static final Logger LOGGER = Logger
			.getLogger(PartitionByMpId.class);

	private Map<Integer, Integer> map = null;

	@Override
	public void init() {
		// initFromBAS();

	}

	private void initFromBAS() {
		try {
			Properties prop = new Properties();
			prop.load(CacheService.class.getResourceAsStream(MPID_PROPERTIES));
			String basUrl = prop.getProperty("partitionByMpId");
			
			LOGGER.info("get PartitionByMpId rule from url : " + basUrl);
			
			URL url = new URL(basUrl);
			HttpURLConnection urlConnection = (HttpURLConnection) url
					.openConnection();

			urlConnection.setRequestMethod("GET");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);
			urlConnection.setUseCaches(false);

			InputStream in = urlConnection.getInputStream();
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<HashMap<Integer, Integer>> typeRef = new TypeReference<HashMap<Integer, Integer>>() {
			};

			map = mapper.readValue(in, typeRef);
		} catch (Exception e) {
			// e.printStackTrace();
			LOGGER.error("init PartitionByMpId error", e);
		}
	}

	@Override
	public Integer calculate(String columnValue) {
		if (map == null) {
			initFromBAS();
		}
		Integer mpId = Integer.valueOf(columnValue);
		Integer nodeIndex = map.get(mpId);
		return (nodeIndex == null || nodeIndex <= 0) ? 0 : nodeIndex;
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this,
				beginValue, endValue);
	}

}
