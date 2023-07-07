import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IBM {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().master("local[1]").appName("IBM").getOrCreate();
        Dataset<Row> mySet = spark.read().option("header", "true").option("delimiter",",").csv("src/main/resources/Erasmus.csv");
        mySet.show();
    }
}
