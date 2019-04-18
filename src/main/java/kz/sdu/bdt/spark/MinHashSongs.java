package kz.sdu.bdt.spark;

import kz.sdu.bdt.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class MinHashSongs {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: MinHashSongs <file> <out_file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("MinHashSongs")
//                .config("spark.master", "local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, String> songsAndHashes = lines.mapToPair(s -> {
            String[] fields = s.split("\t");

            return new Tuple2<>(fields[0] + "|" + fields[1], StringUtils.createSongHashes(fields[2]));
        });

        songsAndHashes.saveAsTextFile(args[1]);
        spark.stop();
    }
}
