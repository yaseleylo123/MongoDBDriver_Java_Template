package MongoDB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.print.attribute.standard.DateTimeAtCompleted;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

public class MongoDBDriver {

    private MongoDatabase database;

    private ListCollectionsIterable<Document> collections;
    private MongoIterable<String> collectionNames;

    public MongoDBDriver(MongoDatabase database){
        this.database = database;
        this.collections = this.database.listCollections();
        this.collectionNames = this.database.listCollectionNames() ;
    }

    private boolean isCollectionExists(String collectionname){
        final MongoIterable<String> iterable = this.database.listCollectionNames();
        try(final MongoCursor<String> it = iterable.iterator()) {
            while(it.hasNext()) {
                if(it.next().equalsIgnoreCase(collectionname)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createCollection(String collectionname){
        if(!isCollectionExists(collectionname)){
            this.database.createCollection(collectionname, new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));
            this.collections = this.database.listCollections();
        } else {
            System.out.println("Can not create collection - collectionname: " + collectionname + " already exists");
        }
    }

    public void createCollectionWithIndex(String collectionname, String tag, int index){
        if(!isCollectionExists(collectionname)){
            this.database.getCollection(collectionname).createIndex(new BasicDBObject(tag, index));
            this.collections = this.database.listCollections();
        }
    }

    public void dropCollection(String collectionname){
        if(isCollectionExists(collectionname)){
            MongoCollection<Document> coll = this.database.getCollection(collectionname);
            coll.drop();
        }
        this.collections = this.database.listCollections();
    }

}
