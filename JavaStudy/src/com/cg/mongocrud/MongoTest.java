package com.cg.mongocrud;

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MongoTest {

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    private static DB db = null;

    public static void main(String args[]) throws Exception {
        init();
        //        insert();
        find();
        //        update();
        //        remove();
    }

    private static void init() throws Exception {
        initByURI();
        //        initByCredential();
    }

    /**
     * 插入一条文档
     */
    private static void insert() {
        DBCollection coll = db.getCollection("clo_1");
        try {
            BasicDBObject doc = new BasicDBObject("age", 5).append(
                    "info", new BasicDBObject("version", "6.7"));
            coll.insert(doc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void find() {
        DBCollection coll = db.getCollection("clo_1");
        BasicDBObject query = new BasicDBObject("age", 5);
        //        BasicDBObject query = new BasicDBObject("age", new BasicDBObject("$gt", 6));
        DBCursor cursor = coll.find(query).limit(1);
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }
    }

    private static void update() {
        DBCollection coll = db.getCollection("clo_1");

        BasicDBObject query = new BasicDBObject("age", "5");
        BasicDBObject set = new BasicDBObject("$set", new
                BasicDBObject("info.version", "1.0"));
        coll.update(query, set);
    }

    private static void remove() {
        DBCollection coll = db.getCollection("clo_1");
        BasicDBObject query = new BasicDBObject("age", "5");
        coll.remove(query);
    }

    private static void fsFind() {
        if (db == null) {
            return;
        }
        GridFS gridFS = new GridFS(db);
        List<GridFSDBFile> find = gridFS.find("1456366441428.html");
        System.out.println(find.toString());
    }

    private static void initByURI() {
        String mongoURI = String.format("mongodb://%s:%d", "192.168.255.102", 27017);
        MongoClientURI uri = new MongoClientURI(mongoURI);
        MongoClient mongoClient = new MongoClient(uri);
        db = mongoClient.getDB("test");
    }

    private static void initByCredential() throws UnknownHostException {

        ServerAddress serverAddress = new ServerAddress("192.168.255.102",
                27017);
        //        MongoCredential credential = MongoCredential.createMongoCRCredential(
        //                "kyd", "test", "kyd".toCharArray());
        MongoClient mongoClient = new MongoClient(serverAddress);
        db = mongoClient.getDB("test");
    }

    private static void girdFSTest() throws IOException {
        GridFS gridFS = new GridFS(db);
        File file = new File("C:\\Users\\cheng\\Desktop\\a.txt");
        GridFSInputFile gfsf = gridFS.createFile(file);

        long timeMil = System.currentTimeMillis();
        Date date = new Date();
        String contentType = "html";
        String extension = "." + contentType;
        gfsf.put("contentType", contentType);
        gfsf.put("filename", timeMil + extension);
        gfsf.put("date", format.format(date));
        gfsf.put("time", (int) (date.getTime() / 1000));

        BasicDBObject parameter = new BasicDBObject();
        parameter.append("key", "309001115200016");
        BasicDBObject metadata = new BasicDBObject();
        metadata.append("parameter", parameter);

        gfsf.setMetaData(metadata);
        gfsf.save();
    }
}