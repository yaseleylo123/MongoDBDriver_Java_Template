package MongoDB;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MongoConnection {

    private MongoClient client;
    private MongoDatabase database;

    public MongoConnection(String configfilepath){
        this.init(configfilepath);
    }

    public void init(String configfilepath){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configfilepath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ipaddress = properties.getProperty("ipaddress");
        int port = Integer.parseInt(properties.getProperty("port"));
        String databasename = properties.getProperty("databasename");
        this.client = new MongoClient(ipaddress, port);
        this.database = this.client.getDatabase(databasename);
    }

    public MongoClient getClient(){
        return this.client;
    }

    public void setClient(MongoClient client){
        this.client = client;
    }

    public MongoDatabase getDatabase(){
        return this.database;
    }

    public void setDatabase(MongoDatabase database){
        this.database = database;
    }

    public String getDatabasename(){
        return this.database.getName();
    }

}
