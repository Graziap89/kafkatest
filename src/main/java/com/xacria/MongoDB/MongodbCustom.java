package com.xacria.MongoDB;

import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.kafka.common.protocol.types.Field;
import org.bson.Document;

import java.util.Arrays;

public class MongodbCustom {
    private  MongoCollection<Document> collection;
    MongodbCustom(){}

    public void connect(){
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("Serie");
        collection = database.getCollection("Record");

    }

    public void insert( String key, String value){

        collection.bulkWrite(
                Arrays.asList(
                        new UpdateOneModel<>(new Document("serie", 1), new Document("key", key) .append("value", value), new UpdateOptions().upsert(true))
                ));
    }
    public FindIterable<Document>  select(String value) {
        FindIterable<Document> res = collection.find(new Document("value", value));
                return res;

    }


    public void disconnect(MongoClient mongoClient){
        mongoClient.close();
    }


}
