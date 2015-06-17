package org.opencloudb.route.function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByMonthMSDateTest {
	@Test
	public void test() throws ParseException {
		
		PartitionByMonthMSDate partition = new PartitionByMonthMSDate();

		partition.setDateFormat("yyyy-MM-dd");
		partition.setsBeginDate("2015-04-01");

		partition.init();

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Assert.assertEquals(true, 0 == partition.calculate(String.valueOf(df.parse("2015-04-02").getTime())));
		Assert.assertEquals(true, 1 == partition.calculate(String.valueOf(df.parse("2015-05-02").getTime())));
		Assert.assertEquals(true, 2 == partition.calculate(String.valueOf(df.parse("2015-06-02").getTime())));
		Assert.assertEquals(true, 3 == partition.calculate(String.valueOf(df.parse("2015-07-02").getTime())));
		Assert.assertEquals(true, 15 == partition.calculate(String.valueOf(df.parse("2016-07-02").getTime())));

	}
}
