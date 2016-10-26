

package com.adbms.tweets;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo; 
import com.mongodb.AggregationOutput;
public class TwitterStreaming {

	public static void main(String[] args) {
        try
        {
        	Mongo m = new Mongo("localhost", 27010);
            DB db = m.getDB("twitterdb");
//            int tweetCount = 100;
            
            final DBCollection coll = db.getCollection("tweets");
            
        	ConfigurationBuilder cb = new ConfigurationBuilder();
        	cb.setDebugEnabled(true)
        	.setOAuthConsumerKey("ZJ1w4m9FEuMGpwXJuxChk6Rir")
        	.setOAuthConsumerSecret("y39YuxDwGbmhnQEG6LYFFoHbmbmPxJyshjvNhHfWOLxZ59dkfp")
        	.setOAuthAccessToken("482799474-HoDJlf85L70sad0gW2bEaUvwPDnm6N4XGrFU8k5Q")
        	.setOAuthAccessTokenSecret("3s0zNDtOyNg05kRNJNgNHreabAOd3D5h2gDI1y7OHFEJR");
        	
        	StatusListener listener = new StatusListener(){
                int count = 0;
                public void onStatus(Status status) {
//               		System.out.println(status.getId() +  " : " + status.getSource()+ " : " +status.getCreatedAt()+ " : " + status.getUser().getName() + " : " +status.getText());
                	//System.out.println(status.getUser().getName() + " : " + status.getText());
                	
                	DBObject dbObj = new BasicDBObject();
                	
                	dbObj.put("id_str", status.getId());
                	dbObj.put("name", status.getUser().getName());
                	dbObj.put("text", status.getText());
                	dbObj.put("source", status.getSource());  
                	//dbObj.put("count",status.getRetweetCount());
                	dbObj.put("country", status.getPlace().getCountry());
                	 
                	coll.insert(dbObj);
            //System.out.println(++count);
                	
                
          
	                /*	DBObject country = new BasicDBObject("_id","$country");
	                	country.put("name", new BasicDBObject("$sum", 1));
	                	DBObject group = new BasicDBObject("$group", country);
	                	final DBObject projectFields = new BasicDBObject("name", 0);
	                    projectFields.put("name", "$name");
	                    //	projectFields.put("count", 1);
	                    final DBObject project = new BasicDBObject("$project", projectFields);
	
	                    final AggregationOutput aggregate = coll.aggregate(group, project);
	                    System.out.println(aggregate);
	                	*/
	           }
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
				@Override
				public void onScrubGeo(long arg0, long arg1) {
					// TODO Auto-generated method stub
					
				}
				
				public void onStallWarning(StallWarning arg0) {
					// TODO Auto-generated method stub
					
				}
            };
            
            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            twitterStream.addListener(listener);
            // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
            twitterStream.sample();
            
        }catch(Exception e) {
        	e.printStackTrace();
        }
	}

}
