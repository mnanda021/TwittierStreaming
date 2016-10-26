package com.adbms.tweets;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.QueryBuilder;

public class TwitterMapReduce {

	private Mongo mongo;
	private DB db;
	private MapReduceCommand cmd1;
	private MapReduceCommand cmd2;
	private DBCollection inputCollection;	
	private String map;
	private String reduce;
	private String outputCollection;
	private Map<String, Double> result = new TreeMap<String, Double>();
	private Map<Double, List<String>> top10 = new TreeMap<Double, List<String>>();
	private String lastKey = null;

	public TwitterMapReduce() throws Exception {
    	mongo = new Mongo("192.168.1.100", 27017);
        db = mongo.getDB("twitterdb");
		inputCollection = db.getCollection("tweets");
		outputCollection = "tweetstat";
		map = " function() { " +
				" emit(this.source, {count: 1}) " +
				" }";
		
		reduce = "function(key, values) { " +
				"var sum = 0; " +
                "values.forEach(function(v) { " +
                "sum = sum + v['count'] "+
                "}); " +
                "return {count: sum}} ";

		cmd1 = new MapReduceCommand(inputCollection, map, reduce, outputCollection, MapReduceCommand.OutputType.INLINE, null);
	}
	
	public void run() {
		
		System.out.println("Running map Reduce: Last Key = " +lastKey);
		DBObject obj = new BasicDBObject();
		obj.put("_id", -1);

		MapReduceOutput out = null;
		if( lastKey == null ) {
			out = inputCollection.mapReduce( cmd1 );
			lastKey = inputCollection.find().sort(obj).next().get("_id").toString();
		} else {
			QueryBuilder qb = new QueryBuilder();
			qb.put("_id").greaterThan(new ObjectId(lastKey));
			cmd2 = new MapReduceCommand(inputCollection, map, reduce, 
					outputCollection, MapReduceCommand.OutputType.INLINE, qb.get());
			out = inputCollection.mapReduce( cmd2 );
			lastKey = inputCollection.find().sort(obj).next().get("_id").toString();
		}
		
		
		result = new TreeMap<String, Double>();
		
		double total = 0;
		double count = 0;
		DBObject value = null;
		String name = null;
		String trimmed = null;
		for (DBObject o : out.results()) {
			value = (DBObject)o.get("value");
			count = (Double)value.get("count");
			name = (String)o.get("_id");
			trimmed = trim( name );
			total += count;
			if( result.containsKey(trimmed)) {
				result.put(trimmed, count + result.get(trimmed));
			} else {
				result.put(trimmed, count);
			}
		}
		

		top10 = new TreeMap<Double, List<String>>(Collections.reverseOrder());
		for( String s: result.keySet() ) {
			double val = result.get( s );
			if( top10.containsKey(val) ) {
				top10.get(val).add(s);
			} else {
				List<String> list = new ArrayList<String>();
				list.add(s);
				top10.put(val, list);
			}
		}		 						
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		TwitterMapReduce app = new TwitterMapReduce();
		try {
			while( true ) {
				DateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy-hh_mm_ss");
		        String today = formatter.format(new Date());
				app.run();
				if( !app.top10.isEmpty() ) {
					new TwitterChart("Twitter Stream", today, app.top10);
				}
				Thread.sleep(1000*60*10);  //10 minutes
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String trim(String s) {
		if( s.indexOf("href") != -1) {
			s = s.substring(s.indexOf("href"), s.indexOf("rel") - 2);
			if( s.indexOf("http://") != -1 ) {
				s = s.substring(s.indexOf("http://") + 7);
			} else if( s.indexOf("https://") != -1) {
				s = s.substring(s.indexOf("https://") + 8);
			}
			
			if( s.indexOf("/") != -1 ) {
				s = s.substring(0, s.indexOf("/"));
			}
		}
		
		return s;
	}
	

}
