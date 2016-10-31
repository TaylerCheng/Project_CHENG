package com.cg.mongocrud;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Cheng Guang on 2016/10/26.
 */
public class NewMongoDao {

    private static MongoClient mongoClient = null;
    private static MongoDatabase db = null;

    public static void main(String[] args) {
        init();
        //        insert();
        //        find();
        //        update();
        //        remove();
        close();
    }

    private static void init() {
        // 连接到Mongodb服务
        mongoClient = new MongoClient("127.0.0.1", 27017);
        // 连接到数据库.如果库不存在则创建
        db = mongoClient.getDatabase("space");
    }

    private static void update() {
        Calendar today = Calendar.getInstance();
        Date endDay = today.getTime();
        today.add(Calendar.DAY_OF_YEAR, -6);
        Date insertDay = today.getTime();
        today.add(Calendar.MONTH, -6);
        Date beginDay = today.getTime();
        System.out.println(beginDay);
        System.out.println(insertDay);
        System.out.println(endDay);

        // 选择表.如果表不存在则创建表
        MongoCollection<Document> collection = db
                .getCollection("suggestion");

        // 批量更新。所有包含hotness属性的其hotness值乘以2
        collection.updateMany(Filters.exists("hotness"), new Document(
                "$mul", new Document("hotness", 2)));
        // 批量删除。热度小于10的全部删除掉
        collection.deleteMany(Filters.lte("hotness", 10));

    }

    private static void find() {

        Calendar today = Calendar.getInstance();
        Date endDay = today.getTime();
        today.add(Calendar.DAY_OF_YEAR, -6);
        Date insertDay = today.getTime();
        today.add(Calendar.MONTH, -6);
        Date beginDay = today.getTime();
        System.out.println(beginDay);
        System.out.println(insertDay);
        System.out.println(endDay);

        // 选择表.如果表不存在则创建表
        MongoCollection<Document> collection = db
                .getCollection("suggestion");

        // 按条件查询遍历表
        MongoCursor<Document> cursor = collection.find(
                Filters.and(Filters.gte("updatetime", beginDay),
                        Filters.lt("updatetime", endDay))).iterator();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            if (document.containsKey("companyid")) {
                document.put("hotness", document.getDouble("hotness") + 10);
                // 更新。若包含companyid属性，则热度加10后更新
                collection.updateOne(
                        Filters.eq("_id", document.get("_id")),
                        new Document("$inc", new Document("hotness", 10)));
            }
        }
        // 全表遍历
        cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    private static void insert() {

        Calendar today = Calendar.getInstance();
        Date endDay = today.getTime();
        today.add(Calendar.DAY_OF_YEAR, -6);
        Date insertDay = today.getTime();
        today.add(Calendar.MONTH, -6);
        Date beginDay = today.getTime();
        System.out.println(beginDay);
        System.out.println(insertDay);
        System.out.println(endDay);

        // 选择表.如果表不存在则创建表
        MongoCollection<Document> collection = db
                .getCollection("suggestion");
        // 清空表
        collection.deleteMany(Filters.exists("_id"));
        // 创建索引.复合索引，按type升序，按cont降序
        collection.createIndex(new Document("type", 1).append("cont", -1));
        // 构造Document
        List<Document> list = new ArrayList<Document>();
        Document document1 = new Document();
        document1.put("type", 1);
        document1.put("cont", "数据挖掘");
        document1.put("hotness", 1563.5);
        document1.put("updatetime", insertDay);
        Document document2 = new Document();
        document2.put("type", 2);
        document2.put("cont", "北京百度科技有限公司");
        document2.put("shortcont", "百度");
        document2.put("hotness", 1563.5);
        document2.put("companyid", 41);
        document2.put("updatetime", insertDay);
        Document document3 = new Document();
        document3.put("type", 3);
        document3.put("cont", "华中科技大学");
        document3.put("hotness", 90.7);
        document3.put("updatetime", insertDay);
        Document document4 = new Document();
        list.add(document4);
        list.add(document2);
        list.add(document3);
        // 单条插入
        collection.insertOne(document1);
        // 多条插入
        collection.insertMany(list);
        // 按条件查询遍历表
        MongoCursor<Document> cursor = collection.find(
                Filters.and(Filters.gte("updatetime", beginDay),
                        Filters.lt("updatetime", endDay))).iterator();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            if (document.containsKey("companyid")) {
                document.put("hotness", document.getDouble("hotness") + 10);
                // 更新。若包含companyid属性，则热度加10后更新
                collection.updateOne(
                        Filters.eq("_id", document.get("_id")),
                        new Document("$inc", new Document("hotness", 10)));
            }
        }
        // 批量更新。所有包含hotness属性的其hotness值乘以2
        collection.updateMany(Filters.exists("hotness"), new Document(
                "$mul", new Document("hotness", 2)));
        // 批量删除。热度小于10的全部删除掉
        collection.deleteMany(Filters.lte("hotness", 10));
        // 全表遍历
        cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    private static void remove() {

        // 选择表.如果表不存在则创建表
        MongoCollection<Document> collection = db
                .getCollection("suggestion");

        // 批量删除。热度小于10的全部删除掉
        collection.deleteMany(Filters.lte("hotness", 10));
    }
    //关闭连接
    private static void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
