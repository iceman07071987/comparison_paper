package kz.sdu.bdt.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static kz.sdu.bdt.StringUtils.STOP_WORDS;
import static kz.sdu.bdt.StringUtils.clearString;

public class PopularWordAmongSinger {

    public static void main(String[] args) {
//        args = new String[] {"songdata_tabbed.csv", "out.txt"};
        if (args.length < 2) {
            System.err.println("Usage: PopularWordAmongSinger <file> <out_file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("PopularWordAmongSinger")
//                .config("spark.master", "local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<Tuple2<String, String>> words = lines.flatMap(s -> {
            List<Tuple2<String, String>> ws = new ArrayList<>();
            String[] vs = s.split("\t");
            for (String word : clearString(vs[2]).split(" ")) {
                ws.add(new Tuple2<>(vs[0], word));
            }

            return ws.iterator();
        }).filter(t -> t._2.length() > 3 && !STOP_WORDS.contains(t._2));

        JavaPairRDD<Tuple2<String, String>, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<Tuple2<String, String>, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<String, Tuple2<String, Integer>> maxMap = counts.mapToPair(s -> new Tuple2<>(s._1._1, new Tuple2<>(s._1._2, s._2))).reduceByKey((t1, t2) -> t1._2 > t2._2 ? t1 : t2);

        List<Tuple2<String, Tuple2<String, Integer>>> result = new ArrayList<>(maxMap.collect());
        result.sort(Comparator.comparing(o -> o._1));

        for (Tuple2<String, Tuple2<String, Integer>> tuple : result) {
            System.out.println(tuple._1() + " -> " + tuple._2._1 + "(" + tuple._2._2 + ")");
        }

        spark.stop();
    }
}
