package com.xacria.MongoDB;

import com.mongodb.client.*;
import org.bson.Document;

public class MongodbCustom {
    private  MongoCollection<Document> collection;
    public MongodbCustom(){}

    public void connect(){
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("Serie");
        collection = database.getCollection("Record");
    }

    public void insert( String key, String value){
        collection.insertOne(new Document("key", key).append("value", value));
    }

    public void print(){
        FindIterable<Document> res  = collection.find();

        for (Document document: res)
            System.out.println(document.toJson());
    }

    public FindIterable<Document>  select(String value) {
        FindIterable<Document> res = collection.find(new Document("value", value));
                return res;
    }
    public void clear(){
        collection.drop();
    }

    public void disconnect(MongoClient mongoClient){
        mongoClient.close();
    }


    public static void main(String[] args) {
        MongodbCustom db = new MongodbCustom();
        db.connect();
        db.clear();
        db.print();

    }
}
