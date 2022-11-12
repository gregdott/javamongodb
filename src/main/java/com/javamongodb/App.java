package com.javamongodb;

import java.util.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

//-----------------------------------------------------------------------------
// for removing annoying mongodb log messages
import java.util.logging.Logger;
import java.util.logging.Level;
//-----------------------------------------------------------------------------

/*
 * Author: Gregory Dott
 * 12-11-2022
 * 
 * This is very basic for now. Just simple ways of doing basic db operations on Mongodb in Java.
 * Insert, get, update, delete etc.
 * 
 * Just your average CRUD (CRUDD in this case).
 */

public final class App {
    static String mongoConn = "mongodb://localhost:27017";

    private App() {
    }
    
    public static void main(String[] args) {
        //insertRecord(1, 2, "a whole lot of random text");
        // Document doc = getRecord(1, 2);
        // System.out.println(doc);
        //updateRecord(1, 2, "This is the new text");
        //deleteRecord1(1);
        //deleteRecord2(1, 2);
    }

    /**
     * insertRecord - insert record into relevant db table. 
     * @param verse
     */
    private static void insertRecord(int id1, int id2, String text) {        
        //--------------------------------------------------------------------------------------------------------------------
        try (MongoClient mongoClient = MongoClients.create(mongoConn)) {

            MongoDatabase db = mongoClient.getDatabase("mongotest");
            MongoCollection<Document> coll = db.getCollection("collection1"); 

            Document doc = new Document("_id", new ObjectId());
            doc.append("id1", id1);
            doc.append("id2", id2);
            doc.append("text", text);

            coll.insertOne(doc);
        }
        //--------------------------------------------------------------------------------------------------------------------
    }

    /**
     * getRecord - get a record from the db using multiple fields to select (just to show how...)
     *
     */
    private static Document getRecord(int id1, int id2) {
        //--------------------------------------------------------------------------------------------------------------------
        try (MongoClient mongoClient = MongoClients.create(mongoConn)) {

            MongoDatabase db = mongoClient.getDatabase("mongotest");
            MongoCollection<Document> coll = db.getCollection("collection1"); 

            Map<String, Integer> selector = new HashMap<String, Integer>();
            selector.put("id1", id1);
            selector.put("id2", id2);

            Document doc = coll.find(new Document(selector)).first();
            
            return doc;
        }
        //--------------------------------------------------------------------------------------------------------------------
    }

    /**
     * updateRecord - 
     * @param 
     */
    private static void updateRecord(int id1, int id2, String text) {
        
        
        //-----------------------------------------------------------------------------------------------------------------------------
        // Find the source verse and record the destination verse and its weight in the adj section of the source verse
        try (MongoClient mongoClient = MongoClients.create(mongoConn)) {

            MongoDatabase db = mongoClient.getDatabase("mongotest");
            MongoCollection<Document> coll = db.getCollection("collection1");

            Bson filter1 = eq("id1", id1);
            Bson filter2 = eq("id2", id2);
            Bson update = set("text", text);

            coll.findOneAndUpdate(and(filter1, filter2), update);
        }
        //-----------------------------------------------------------------------------------------------------------------------------
    }

    /**
     * deleteRecord1 - delete a record using single identifying parameter
     * @param id     
     */
    private static void deleteRecord1(int id) {
        //--------------------------------------------------------------------------------------------------------------------
        try (MongoClient mongoClient = MongoClients.create(mongoConn)) {
            MongoDatabase db = mongoClient.getDatabase("mongotest");
            MongoCollection<Document> coll = db.getCollection("collection1"); 
            Bson filter = eq("id1", id);
            DeleteResult result = coll.deleteOne(filter);
            System.out.println(result);
        }
    }

    /**
     * deleteRecord2 - delete a record using multiple identifying parameters (just to show use of 'and')
     * @param id1
     * @param id2
     */
    private static void deleteRecord2(int id1, int id2) {
        //--------------------------------------------------------------------------------------------------------------------
        try (MongoClient mongoClient = MongoClients.create(mongoConn)) {
            MongoDatabase db = mongoClient.getDatabase("mongotest");
            MongoCollection<Document> coll = db.getCollection("collection1"); 
            Bson filter1 = eq("id1", id1);
            Bson filter2 = eq("id2", id2);
            
            DeleteResult result = coll.deleteOne(and(filter1, filter2));
            System.out.println(result);
        }
    }

    
}
