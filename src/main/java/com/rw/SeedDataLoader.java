package com.rw;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.mongoRepo;
import com.rw.persistence.mongoStore;


public class SeedDataLoader {
	 static final Logger log = Logger.getLogger(SeedDataLoader.class.getName());

	private mongoRepo repo = null;
	private mongoStore store = null;
	
	public SeedDataLoader(String tenant) throws Exception {
		if ( tenant == null) 
			tenant = (System.getProperty("PARAM3") == null)? "STN" : System.getProperty("PARAM3");
		repo = new mongoRepo(tenant);
		store = new mongoStore(tenant);		 
	}
	
	public void LoadSeedData(String strColl, String metadatafile) throws Exception
	{
		try {
		      InputStream is = 
		        		this.getClass().getResourceAsStream(metadatafile);
		      String xml;
		 			xml = IOUtils.toString(is);
		 	  JSONObject rwSeed = XML.toJSONObject(xml);
		 	  
		      if ( !rwSeed.isNull("ReferralWireSeedData") ) { 	
		 	  	 JSONObject json = rwSeed.getJSONObject("ReferralWireSeedData");
			     DBCollection dbcoll = store.getColl(strColl);
		 	  	 
		 		 Object coll = (Object) json.get(strColl);
			     if ( coll instanceof JSONArray ) {
					  JSONArray defs = (JSONArray ) coll;
					  for (int i =0; i < defs.length(); i++) {
						  Object obj = (JSONObject) defs.get(i);
						  dbcoll.insert( (DBObject) com.mongodb.util.JSON.parse(obj.toString()) );
					  }
				  }
				  else {
					  dbcoll.insert( (DBObject) com.mongodb.util.JSON.parse(coll.toString()) );
				  }
		      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
	}
	
	public void LoadSavedSearch(String datafile,String mongoTableName, String xmlCollection) throws Exception
	{
		//String mongoTableName = "rwSavedSearch";
		//String xmlCollection = "SavedSearches";
		try {
			  InputStream is = 
		        		this.getClass().getResourceAsStream(datafile);
		      String xml;
		 			xml = IOUtils.toString(is);
		 	  JSONObject rwSeed = XML.toJSONObject(xml);
		 	  
		      if ( !rwSeed.isNull(xmlCollection) ) { 	
		 	  	 JSONObject json = rwSeed.getJSONObject(xmlCollection );
			     DBCollection dbTable = store.getColl(mongoTableName);
		 	  	 
		 		 Object xmlRecords = (Object) json.get(mongoTableName);
		 		if ( xmlRecords instanceof JSONArray == false) {
		 			JSONObject xmlRecord = (JSONObject)xmlRecords;
		 			JSONArray jAry = new JSONArray();
		 			jAry.put(xmlRecord);
		 			xmlRecords = jAry;
		 		}
		 		 
			     if ( xmlRecords instanceof JSONArray ) {
					  JSONArray xmlRecordAry = (JSONArray ) xmlRecords;
					  for (int i =0; i < xmlRecordAry.length(); i++) {
						  JSONObject xmlRecord = (JSONObject) xmlRecordAry.get(i);
						  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecord.toString()));
					  }
				  }
				  else {
					  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecords.toString()) );
				  }
		      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
	}
	
	public void LoadLOVMaps(String mapfile)throws Exception
	{
		
		String mongoTableName = "rwLOVMap";
		String xmlCollection = "SeedLOVMaps";
		try {
			  InputStream is = 
		        		this.getClass().getResourceAsStream(mapfile);
		      String xml;
		 			xml = IOUtils.toString(is);
		 	  JSONObject rwSeed = XML.toJSONObject(xml);
		 	  
		      if ( !rwSeed.isNull(xmlCollection) ) { 	
		 	  	 JSONObject json = rwSeed.getJSONObject(xmlCollection );
			     DBCollection dbTable = store.getColl(mongoTableName);
		 	  	 
		 		 Object xmlRecords = (Object) json.get(mongoTableName);
		 		if ( xmlRecords instanceof JSONArray == false) {
		 			JSONObject xmlRecord = (JSONObject)xmlRecords;
		 			JSONArray jAry = new JSONArray();
		 			jAry.put(xmlRecord);
		 			xmlRecords = jAry;
		 		}
		 		 
			     if ( xmlRecords instanceof JSONArray ) {
					  JSONArray xmlRecordAry = (JSONArray ) xmlRecords;
					  for (int i =0; i < xmlRecordAry.length(); i++) {
						  JSONObject xmlRecord = (JSONObject) xmlRecordAry.get(i);
						  
						  String parent_GlobalVal = xmlRecord.getString("parent_GlobalVal");
						  String child_GlobalVal = xmlRecord.getString("child_GlobalVal"); 
						  String parent_LovType = xmlRecord.getString("parent_LovType");
						  String child_LovType = xmlRecord.getString("child_LovType");
						  String childId = getLOVId(child_LovType,child_GlobalVal);
						  String parentId = getLOVId(parent_LovType,parent_GlobalVal);
						  xmlRecord.put("parentId", parentId);
						  xmlRecord.put("childId", childId);
						  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecord.toString()));
					  }
				  }
				  else {
					  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecords.toString()) );
				  }
		      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
			
		
	}
	
	public String getLOVId(String LovType,String GlobalVal) throws Exception{
		  String retVal = new ObjectId().toString();
		  QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("LovType").is(LovType).get(),new QueryBuilder().start().put("GlobalVal").is(GlobalVal).get());
		  BasicDBObject query = new BasicDBObject();
		  query.putAll(qm.get());
		  BasicDBList lovItem = (BasicDBList)GetDBList("rwLov",query);
		  if (lovItem != null && lovItem.size() >0){
			  BasicDBObject result = (BasicDBObject)lovItem.get(0);
			  retVal = result.get("_id").toString();
		  }
		  return retVal;
	}
	
	public BasicDBList GetDBList (String tableName, BasicDBObject query) throws Exception{
		//BasicDBObject query = new BasicDBObject();
		BasicDBObject emptyRecord = new BasicDBObject();
		BasicDBList recordSet = new BasicDBList();; 
        DBCursor cursorDoc = null;
		DBObject sortSpec = null;
		if ( (sortSpec != null )  && !sortSpec.keySet().isEmpty()) {
			// sorting is optional
			cursorDoc = store.getColl(tableName).find(query,emptyRecord).sort(sortSpec);
		}
		else if (query != null) {	
			cursorDoc = store.getColl(tableName).find(query,emptyRecord);
		} else {
			cursorDoc = store.getColl(tableName).find();
		}
		recordSet.clear();
		int recordCount = 0;
		while (cursorDoc.hasNext()) {
			BasicDBObject rec = (BasicDBObject) cursorDoc.next(); 
			recordSet.add(rec);
			recordCount++;
		}
		return recordSet;
	}

	public void CleanupSeedData() throws Exception
	{
		store.getColl("rwLov").drop();
		store.getColl("rwLOVMap").drop();
		store.getColl("rwSavedSearch").drop();
		store.getColl("rwDQRule").drop();
	}
	
	public void ResetSeedData(String coll) throws Exception
	{
		store.getColl(coll).drop();
	}
	
	public void ExportMongoCollection(String mongoCollectionName, String xmlCollectionName, String xmlOutputFileName,HashSet fields) throws Exception{
		BasicDBList mongoCollection = GetDBList(mongoCollectionName,null);

		try {
			 
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
	 
			// root elements
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement(xmlCollectionName);
			doc.appendChild(rootElement);
	 
			
			for (int i = 0; i < mongoCollection.size(); i++){
				Element thisElement = doc.createElement(mongoCollectionName);//the xml nodes are named after the mongoColleciton
				
				BasicDBObject record = (BasicDBObject)mongoCollection.get(i);
				Object attributeArray[] = fields.toArray();
				for (int j = 0; j <fields.size(); j++){
					String attribName = (String)attributeArray[j];
					String attribValue = record.getString(attribName);
					//Attr thisAttr = doc.createAttribute(attribName);
					//thisAttr.setValue(attribValue);
					thisElement.setAttribute(attribName,attribValue);
				}
				rootElement.appendChild(thisElement);
			}
	 
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			String filePath = System.getProperty("user.home") + "/" + xmlOutputFileName;
			
			
			File f = new File(filePath);
			//StreamResult result = new StreamResult(new File("/" + xmlOutputFileName));
			StreamResult result = new StreamResult(f.toURI().getPath());
	 
			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);
	 
			transformer.transform(source, result);
	 
			System.out.println("File saved!");
	 
		  } catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		  } catch (TransformerException tfe) {
			tfe.printStackTrace();
		  }
	}
	
	public void LoadSTNSpeakers(String mapfile)throws Exception
	{
		
		String mongoTableName = "rwParty";
		String xmlCollection = "SeedLOVMaps";
		try {
			  InputStream is = 
		        		this.getClass().getResourceAsStream(mapfile);
		      String xml;
		 			xml = IOUtils.toString(is);
		 	  JSONObject rwSeed = XML.toJSONObject(xml);
		 	  
		      if ( !rwSeed.isNull(xmlCollection) ) { 	
		 	  	 JSONObject json = rwSeed.getJSONObject(xmlCollection );
			     DBCollection dbTable = store.getColl(mongoTableName);
		 	  	 
		 		 Object xmlRecords = (Object) json.get(mongoTableName);
		 		if ( xmlRecords instanceof JSONArray == false) {
		 			JSONObject xmlRecord = (JSONObject)xmlRecords;
		 			JSONArray jAry = new JSONArray();
		 			jAry.put(xmlRecord);
		 			xmlRecords = jAry;
		 		}
		 		 
			     if ( xmlRecords instanceof JSONArray ) {
					  JSONArray xmlRecordAry = (JSONArray ) xmlRecords;
					  for (int i =0; i < xmlRecordAry.length(); i++) {
						  JSONObject xmlRecord = (JSONObject) xmlRecordAry.get(i);
						  
						  String parent_GlobalVal = xmlRecord.getString("parent_GlobalVal");
						  String child_GlobalVal = xmlRecord.getString("child_GlobalVal"); 
						  String parent_LovType = xmlRecord.getString("parent_LovType");
						  String child_LovType = xmlRecord.getString("child_LovType");
						  String childId = getLOVId(child_LovType,child_GlobalVal);
						  String parentId = getLOVId(parent_LovType,parent_GlobalVal);
						  xmlRecord.put("parentId", parentId);
						  xmlRecord.put("childId", childId);
						  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecord.toString()));
					  }
				  }
				  else {
					  dbTable.insert( (DBObject) com.mongodb.util.JSON.parse(xmlRecords.toString()) );
				  }
		      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
			
		
	}
	
}
