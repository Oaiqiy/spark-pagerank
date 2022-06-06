package dev.oaiqiy;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class PageRank {

    private static final int count = 10;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("PageRank1");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        String rankPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\dataset\\pr.txt";
//        String linkPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\dataset\\web-Google.txt";
//        String outputPath = "F:\\javaProject\\spark-pagerank\\src\\main\\resources\\output.txt";

        String linkPath = args[0];
        String outputPath = args[1];

        JavaPairRDD<String,String> source = sc.textFile(linkPath).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] kv = s.split("\\s+|\\t");
                return new Tuple2<>(kv[0],kv[1]);
            }
        });

        JavaPairRDD<String,Iterable<String>> links = source.distinct().groupByKey().cache();

        JavaPairRDD<String,Double> ranks = source.keys().union(source.values()).distinct().mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                return new Tuple2<>(s, 1.0);
            }
        });


//        JavaPairRDD<String, Double> ranks = sc.textFile(rankPath).mapToPair(
//                (PairFunction<String, String, Double>) s -> {
//                    String[] kv = s.split("\t");
//                    return new Tuple2<>(kv[0], 1.0);
//        });
//
//        JavaPairRDD<String, Iterable<String>> links = sc.textFile(linkPath).mapToPair(
//                (PairFunction<String, String, Iterable<String>>) s -> {
//                    String[] kv = s.split("\\s{2,}|\t");
//                    String[] values = kv[1].split(",");
//                    return new Tuple2<>(kv[0], Arrays.asList(values));
//        });


        for(int i = 0;i < count;i++){

//            JavaPairRDD<String, Double> contributions = links.join(ranks).values().flatMapToPair(
//                    (PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>) tuple2 -> {
//
//                        int targetCount = Iterables.size(tuple2._1);
//
//                        List<Tuple2<String,Double>> result = new ArrayList<>(targetCount);
//                        double v = tuple2._2 / targetCount;
//                        for(String s : tuple2._1)
//                            result.add(new Tuple2<>(s,v));
//
//                        return result.iterator();
//            });

            JavaPairRDD<String, Double> contributions = links.rightOuterJoin(ranks).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Optional<Iterable<String>>, Double>>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<String, Tuple2<Optional<Iterable<String>>, Double>> tuple) throws Exception {
                    String k = tuple._1;

                    Tuple2<Optional<Iterable<String>>, Double> tuple2 = tuple._2;
                    if(tuple2._1.isPresent()){
                        Iterable<String> iterable = tuple2._1.get();

                        int targetCount = Iterables.size(iterable);

                        List<Tuple2<String,Double>> result = new ArrayList<>(targetCount);
                        double v = tuple2._2 / targetCount;
                        for(String s : iterable)
                            result.add(new Tuple2<>(s,v));

                        result.add(new Tuple2<>(k,0.0));
                        return result.iterator();
                    }else{
                        List<Tuple2<String,Double>> result = new ArrayList<>(1);
                        result.add(new Tuple2<>(k,0.0));
                        return result.iterator();
                    }

                }
            });

            ranks = contributions.reduceByKey((Function2<Double, Double, Double>) Double::sum)
                    .mapValues(sum -> 0.15 + sum * 0.85);

        }


        List<Tuple2<String,Double>> output = ranks.collect();

        sc.stop();

        System.out.println("stop");

        // sort output list
        output = new ArrayList<>(output);
        output.sort((o1, o2) -> Double.compare(o2._2,o1._2));

        // write file
        Output.outputList(output,outputPath);

    }
}
