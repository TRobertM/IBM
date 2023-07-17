import org.apache.spark.sql.*;

public class IBM {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().master("local[1]").appName("IBM").getOrCreate();
        Dataset<Row> mySet = spark.read().option("header", "true").option("delimiter",",").csv("src/main/resources/Erasmus.csv");
        mySet = mySet.groupBy("Receiving Country Code","Sending Country Code").agg(functions.count("Sending Country Code").alias("Students"))
                .orderBy("Receiving Country Code").filter(functions.col("Receiving Country Code").equalTo("FR"));


        String url = "jdbc:postgresql://localhost:5432/ibm";
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");

        mySet.write().mode(SaveMode.Overwrite).format("jdbc").option("url", url).option("user", user).option("password",password).option("dbtable", "FRANCE").save();

        mySet = spark.read().option("header", "true").option("delimiter",",").csv("src/main/resources/Erasmus.csv");
        mySet = mySet.groupBy("Receiving Country Code","Sending Country Code").agg(functions.count("Sending Country Code").alias("Students"))
                .orderBy("Receiving Country Code").filter(functions.col("Receiving Country Code").equalTo("RO"));

        mySet.write().mode(SaveMode.Overwrite).format("jdbc").option("url", url).option("user", user).option("password",password).option("dbtable", "ROMANIA").save();

        mySet = spark.read().option("header", "true").option("delimiter",",").csv("src/main/resources/Erasmus.csv");
        mySet = mySet.groupBy("Receiving Country Code","Sending Country Code").agg(functions.count("Sending Country Code").alias("Students"))
                .orderBy("Receiving Country Code").filter(functions.col("Receiving Country Code").equalTo("BE"));

        mySet.write().mode(SaveMode.Overwrite).format("jdbc").option("url", url).option("user", user).option("password",password).option("dbtable", "BELGIUM").save();
    }
}