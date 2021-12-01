package com.splunk.steveleung.kafka.consumertomongodb;

import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReviewsConsumerToMongo {
    public static void main(String[] args) throws IOException, InterruptedException {
        //Establish connection to MongoDB
        String connectionString = "mongodb://mongodb/O11y";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
            databases.forEach(db -> System.out.println(db.toJson()));
            MongoCredential credential; // Creating Credentials
            credential = MongoCredential.createCredential("O11y", "O11y", "O11y".toCharArray());
            System.out.println("Connected to the database successfully");
            MongoDatabase O11yDB = mongoClient.getDatabase("O11y"); // Accessing the database
            MongoCollection O11yCollection = O11yDB.getCollection("O11yCollection");
//        Document myDoc = Document.parse(jsonString);

            // Set up Kafka Connection
            Logger logger = LoggerFactory.getLogger(ReviewsConsumerToMongo.class.getName());
            String bootstrapServers = "kafka-0.kafka-headless.default.svc.cluster.local:9092";
            String groupId = "reviews_to_db";
            String topic = "reviews";
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", bootstrapServers);
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", StringDeserializer.class.getName());
            properties.setProperty("group.id", groupId);
            properties.setProperty("auto.offset.reset", "earliest");
            KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
            consumer.subscribe(Arrays.asList(topic));

            // Consume continously from Reviews topic
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                Iterator var8 = records.iterator();
                String var_key;
                ConsumerRecord record;
                int var_partition;
                var8 = records.iterator();
                while (var8.hasNext()) {
                    record = (ConsumerRecord) var8.next();
                    PrintStream var10000 = System.out;
                    var_key = (String) record.key();
    //                var10000.println("Key: " + var_key + ", Value: " + (String)record.value());
                    var10000 = System.out;
                    var_partition = record.partition();
    //                var10000.println("Partition: " + var_partition + ", Offset:" + record.offset());
                    //get sentiment
                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://sentiment:5001")).build();
                    HttpResponse<String> sentiment = client.send(request, HttpResponse.BodyHandlers.ofString());

                    //Insert into MongoDB
                    Document myDoc = Document.parse((String)record.value());
                    myDoc.append("sentiment", (String)sentiment.body());
                    O11yCollection.insertOne(myDoc);

                }
            }
    }
    }
}
