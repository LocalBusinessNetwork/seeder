package com.rw;

import java.util.Date;
import java.util.Iterator;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoMaster;
import com.rw.persistence.mongoRepo;
import com.rw.persistence.mongoStore;

public class SeedData {

    /**
	 * @param args
	 * @throws Exception 
	 * Usage 1 :  SchemaUpdate -connection dbserver -tenant tenantSuffix -version versionnumber 
	 * Usage 2 :  SchemaUpdate -connection dbserver -tenant tenantSuffix -coll collection -file filename -mode [add/reset]
	 * java -classpath ${RWHOME}/Web/target/web/WEB-INF/lib -jar -DPARAM3=STN -DTENANTPROXY=successfulthinkersnetwork.com ${RWHOME}/Web/target/web/WEB-INF/lib/SchemaLoader-DROP1.jar
 	 *  Examples :
 	 *  java -classpath ${RWHOME}/Web/target/web/WEB-INF/lib -jar -DPARAM3=STN -DTENANTPROXY=successfulthinkersnetwork.com ${RWHOME}/Web/target/web/WEB-INF/lib/SeedLoader-V1.jar -coll rwLov -file /urgency.xml -mode add
 	 *  java -classpath ${RWHOME}/Web/target/web/WEB-INF/lib -jar -DPARAM3=STN -DTENANTPROXY=successfulthinkersnetwork.com ${RWHOME}/Web/target/web/WEB-INF/lib/SeedLoader-V1.jar -coll rwDQRule -file /dqRules.xml -mode reset
 	 *
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	  	
		String rwhome = System.getenv("RWHOME");
      	System.out.println("RWHOME = " + rwhome);   	     	
      	System.setProperty("WebRoot",rwhome + "/web/src/main/webapp");
      	
		System.setProperty("JDBC_CONNECTION_STRING","localhost");
    	System.setProperty("EMAIL","local"); // Run demo setup always in non-email mode.
       	System.setProperty("VERSION","1.0"); // Run demo setup always in non-email mode.
	
		Options options = new Options();		
		
		options.addOption("connection", true, "DB server Connection");
		options.addOption("tenant", true, "Tenant Suffix");
		options.addOption("version", true, "version");
		options.addOption("file", true, "file Name");
		options.addOption("coll", true, "coll");
		options.addOption("mode", true, "mode");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse( options, args);
  
		if(cmd.hasOption("connection")){ 
			System.setProperty("JDBC_CONNECTION_STRING", cmd.getOptionValue("connection"));
		}
		
		if(cmd.hasOption("tenant")){ 
			System.setProperty("PARAM3", cmd.getOptionValue("tenant"));
		}

		if(cmd.hasOption("version")){ 
			System.setProperty("VERSION", cmd.getOptionValue("version"));
		}
		
		String tenant = System.getProperty("PARAM3");
	   	if ( tenant == null ) {
	   		System.out.println("Tenant Not specified, exiting");
	   		return;
	   	}
	   	else 
			System.out.println("TENANT = " + tenant);  			
	   	
	   	if(cmd.hasOption("file")){ 
	   		String fileName = cmd.getOptionValue("file");
	   		String coll = cmd.getOptionValue("coll");
	   		String mode = cmd.getOptionValue("mode");
	   		
	   		if ( mode.equals("add")) {
	   			AddSeeddataFromFile(System.getProperty("PARAM3"),coll,fileName);
	   		}
	   		else if (mode.equals("reset")) {
	   			ResetSeeddataFromFile(System.getProperty("PARAM3"),coll,fileName);
	   		}
	   		return;
		}
		
	   	LoadAllSeeddata(System.getProperty("PARAM3"));
    	
    	mongoMaster s = new mongoMaster();
		DBCollection c = s.getColl("rwAdmin");
		BasicDBObject query = new BasicDBObject();
		query.put("tenant", tenant);
		DBObject found = c.findOne(query);
		if ( found != null ) {
			found.put("ver", System.getProperty("VERSION"));
		}
    	BasicDBObject index = new BasicDBObject();
    	mongoStore d = new mongoStore(tenant);
		
    	index.put("GeoLocation", "2dsphere");
    	// s.getColl("rwParty").ensureIndex(index);

    	
	}

	public static void LoadAllSeeddata(String tenant) throws Exception {

		// Load seed data
		SeedDataLoader sl = new SeedDataLoader(tenant);
		
		sl.CleanupSeedData();
		sl.LoadSeedData("rwLov", "/LovSeedData.xml");
		sl.LoadSeedData("rwLOVMap", "/LovSeedData.xml");
		sl.LoadLOVMaps("/SeedLOVMaps.xml");
		sl.LoadSeedData("rwDQRule", "/dqRules.xml");
		sl.LoadSavedSearch("/SavedSearchesSeed.xml","rwSavedSearch","SavedSearches");
		sl.LoadSavedSearch("/ReportsSeed.xml","rwReport","Reports");

	}
	
	public static void AddSeeddataFromFile(String tenant, String coll, String fileName) throws Exception {
		SeedDataLoader sl = new SeedDataLoader(tenant);
		sl.LoadSeedData(coll,fileName );	
	}
	
	public static void ResetSeeddataFromFile(String tenant, String coll, String fileName) throws Exception {
		SeedDataLoader sl = new SeedDataLoader(tenant);
		sl.ResetSeedData(coll);
		sl.LoadSeedData(coll,fileName );	
	}
}
