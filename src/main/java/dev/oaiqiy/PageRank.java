package dev.oaiqiy;

import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class PageRank {

    private static final int count = 30;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("PageRank1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String rankPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\dataset\\pr.txt";
        String linkPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\dataset\\transition.txt";
        String outputPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\output.txt";

        JavaPairRDD<String, Double> ranks = sc.textFile(rankPath).mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] kv = s.split("\t");

                return new Tuple2<>(kv[0], 1.0);
            }
        });

        JavaPairRDD<String, Iterable<String>> links = sc.textFile(linkPath).mapToPair((PairFunction<String, String, Iterable<String>>) s -> {
            String[] kv = s.split("\\s{2,}|\t");
            String[] values = kv[1].split(",");
            return new Tuple2<>(kv[0], Arrays.asList(values));
        });

        for(int i = 0;i < count;i++){

            JavaPairRDD<String, Double> contributions = links.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double>  tuple2) throws Exception {
                    int targetCount = Iterables.size(tuple2._1);

                    List<Tuple2<String,Double>> result = new ArrayList<>(targetCount);
                    double v = tuple2._2 / targetCount;
                    for(String s : tuple2._1)
                        result.add(new Tuple2<>(s,v));

                    return result.iterator();
                }
            });

            ranks = contributions.reduceByKey((Function2<Double, Double, Double>) Double::sum)
                    .mapValues(sum -> 0.15 + sum * 0.85);

        }

        List<Tuple2<String,Double>> output = ranks.collect();

        sc.stop();

        output = new ArrayList<>(output);
        output.sort((o1, o2) -> Double.compare(o2._2,o1._2));


        Output.outputList(output,outputPath);


        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

    }
}
