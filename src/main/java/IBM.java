import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class IBM {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().master("local[1]").appName("IBM").getOrCreate();
        Dataset<Row> mySet = spark.read().option("header", "true").option("delimiter",",").csv("src/main/resources/Erasmus.csv")
                .groupBy("Receiving Country Code", "Sending Country Code").agg(functions.count("Sending Country Code").alias("Students"))
                .orderBy("Receiving Country Code", "Sending Country Code")
                // Remove this line to see all available countries or change the "FR" to the desired country
                .filter(functions.col("Receiving Country Code").equalTo("FR"));
        mySet.show();
    }
}