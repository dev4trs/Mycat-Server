package org.opencloudb.route.function;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.opencloudb.cache.CacheService;

public class PartitionByMpIdTest {
	@Test
	public void test() throws IOException {
		
		Properties prop = new Properties();
		prop.load(CacheService.class.getResourceAsStream("/bas.properties"));
		String basUrl = prop.getProperty("partitionByMpId");
		System.out.println(basUrl);

	}
}
