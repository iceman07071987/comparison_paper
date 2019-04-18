package kz.sdu.bdt.spark;

import kz.sdu.bdt.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class JaccardSimilarity {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JaccardSimilarity <file> <out_file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JaccardSimilarity")
//                .config("spark.master", "local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<Tuple2<String, String>> songs = lines.map(s -> {
            String[] vals = s.split("\t");
            return new Tuple2<>(vals[0], vals[1]);
        });

        JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> combinations = songs.cartesian(songs);

        JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> similar = combinations.filter(s -> {
            String[] h1 = s._1._2.split(" ");
            String[] h2 = s._2._2.split(" ");

            return !s._1._1.equals(s._2._1) && StringUtils.jaccardSimilarity(h1, h2) > 0.7;
        });

        JavaPairRDD<String, String> result = similar.mapToPair(s -> new Tuple2<>(s._1._1, s._2._1));

        result.saveAsTextFile(args[1]);
        spark.stop();
    }
}
