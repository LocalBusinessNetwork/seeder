package com.rw.seeddata.test;


import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.SeedDataLoader;


public class SeedDataLoadTest {

	@Before
	public void setUp() throws Exception {
	}
	
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		
		SeedDataLoader sl = new SeedDataLoader("STN");
		// sl.CleanupSeedData();
		try {
			sl.LoadSeedData("rwDQRule", "/dqRules.xml");
			// sl.LoadSeedData("rwLov", "/urgency.xml");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
