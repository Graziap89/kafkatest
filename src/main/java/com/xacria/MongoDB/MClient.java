package com.xacria.MongoDB;
import com.mongodb.BasicDBObject;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class MClient {


    public static void main(String[] args) {
  /*
    //Connect to a MongoDB Deployment
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

     //Access a Collection

    // Pass BasicDBObject.class as the second argument
        MongoCollection<BasicDBObject> collection = database.getCollection("mycoll", BasicDBObject.class);

    // insert a document
        BasicDBObject document = new BasicDBObject("x", 1);
        collection.insertOne(document);
        document.append("x", 2).append("y", 3);

        Drop a Collection collection.drop();

      //Document Validation
            ValidationOptions collOptions = new ValidationOptions().validator(
            Filters.or(Filters.exists("email"), Filters.exists("phone")));
            database.createCollection("contacts",
            new CreateCollectionOptions().validationOptions(collOptions));*/

    //Get A List of Collections: for (String name : database.listCollectionNames()) System.out.print(name);

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("restaurants");



     //Documento
        Document document = new Document("Serie", 1)
                .append("data", new Document("Data",  "28-07-2019"))
                        .append("value", "5");

        FindIterable<Document> res = collection.find(new Document("data", new Document("Serie", 1)));
        System.out.println("start printing - Serie 1");
        for (Document document1: res)
            System.out.println(document.toJson());
        System.out.println("end printing");


    /*query

        FindIterable<Document> res = collection.find(new Document("stars", new Document("$gte", 2)
                .append("$lt", 5))
                .append("categories", "Bakery"));

        System.out.println("start printing");
        for (Document document: res)
            System.out.println(document.toJson());
        System.out.println("end printing");

    // --------------------------------- //
        Document document = new Document("name", "Café Con Leche")
                .append("contact", new Document("phone", "228-555-0149")
                        .append("email", "cafeconleche@example.com")
                        .append("location", Arrays.asList(-73.92502, 40.8279556)))
                        .append("stars", 3)
                        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));

        FindIterable<Document> res1 = collection.find(new Document("name", "Café Con Leche"));

        System.out.println("Res1 printing");
        for (Document document1: res1)
            System.out.println(document1.toJson());
        System.out.println("Res1 end printing");

        // inserisce un documento: collection.insertOne(document);

        Document doc1 = new Document("name", "Amarcord Pizzeria")
                        .append("contact", new Document("phone", "264-555-0193")
                        .append("email", "amarcord.pizzeria@example.net")
                        .append("location",Arrays.asList(-73.88502, 40.749556)))
                        .append("stars", 2)
                        .append("categories", Arrays.asList("Pizzeria", "Italian", "Pasta"));

        FindIterable<Document> res2 = collection.find(new Document("name", "Amarcord Pizzeria"));

        System.out.println("Res2 printing");
        for (Document document2: res2)
            System.out.println(document2.toJson());
        System.out.println("Res2 end printing");

        Document doc2 = new Document("name", "Blue Coffee Bar")
                .append("contact", new Document("phone", "604-555-0102")
                        .append("email", "bluecoffeebar@example.com")
                        .append("location",Arrays.asList(-73.97902, 40.8479556)))
                .append("stars", 5)
                .append("categories", Arrays.asList("Coffee", "Pastries"));


        FindIterable<Document> res3 = collection.find(new Document("name", "Alue Coffee Bar"));
        System.out.println("Res3 printing");
        for (Document document3: res3)
            System.out.println(document3.toJson());
        System.out.println("Res3 end printing");


    // inserire più documenti:
        List<Document> documents = new ArrayList<Document>();
        documents.add(document);
        documents.add(doc1);
        documents.add(doc2);
        collection.insertMany(documents);

    // update a single document:
        collection.updateOne(
                eq("_id", new ObjectId("57506d62f57802807471dd41")),
                combine(set("stars", 1), set("contact.phone", "228-555-9999"), currentDate("lastModified")));

        FindIterable<Document> res4 = collection.find(new Document("_id", "57506d62f57802807471dd41"));

        System.out.println("Res4 printing");
        for (Document document4: res4)
            System.out.println(document4.toJson());
        System.out.println("Res4 end printing");

    // delete:
        DeleteResult resDel1 = collection.deleteMany(eq("name", "Café Con Leche"));
        DeleteResult resDel2 = collection.deleteMany(eq("name", "Café Con Leche"));
        DeleteResult resDel3 = collection.deleteMany(eq("name", "Amarcord Pizzeria"));

        System.out.println(resDel1.getDeletedCount());
        System.out.println(resDel2.getDeletedCount());
        System.out.println(resDel3.getDeletedCount());

        System.out.println("Order");

      // 1. Ordered bulk operation - order is guaranteed

            collection.bulkWrite(
                Arrays.asList(
                        //new InsertOneModel<>(new Document("_id", 4)),
                       //new InsertOneModel<>(new Document("_id", 5)),
                        //new InsertOneModel<>(new Document("_id", 6)),
                        new UpdateOneModel<>(new Document("_id", 1), new Document("$set", new Document("x", 2)),  new UpdateOptions().upsert(true))
                       // ,new DeleteOneModel<>(new Document("_id", 2)),
                        //new ReplaceOneModel<>(new Document("_id", 3), new Document("_id", 3).append("x", 4))
                ));

/*
       // 2. Unordered bulk operation - no guarantee of order of operation
        collection.bulkWrite(
                Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                        new InsertOneModel<>(new Document("_id", 5)),
                        new InsertOneModel<>(new Document("_id", 6)),
                        new UpdateOneModel<>(new Document("_id", 1),
                                             new Document("$set", new Document("x", 2))),
                        new DeleteOneModel<>(new Document("_id", 2)),
                        new ReplaceOneModel<>(new Document("_id", 3),
                                new Document("_id", 3).append("x", 4))),
                new BulkWriteOptions().ordered(false));*/
     //aggregation
        /*
        AggregateIterable<Document> res5 = collection.aggregate(
                Arrays.asList(
                        Aggregates.match(eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1))
                )
        );

        System.out.println("Res5 printing");
        for (Document document5: res5)
            System.out.println(document5.toJson());
        System.out.println("Res5 end printing");

        AggregateIterable<Document> res6 = collection.aggregate(
                Arrays.asList(
                        Aggregates.project(
                                Projections.fields(
                                        Projections.excludeId(),
                                        Projections.include("name"),
                                        Projections.computed(
                                                "firstCategory",
                                                new Document("$arrayElemAt", Arrays.asList("$categories", 0))
                                        )
                                )
                        )
                )
        );
        System.out.println("Res6 printing");
        for (Document document6: res6)
            System.out.println(document6.toJson());
        System.out.println("Res5 end printing");*/


    }
}
