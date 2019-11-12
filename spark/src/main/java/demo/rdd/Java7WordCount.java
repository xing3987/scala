package demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
/**
 * 在spark中运行（去掉setMaster(**),放开读取和存储路径）
 * /data/spark-2.3.3# bin/spark-submit --master spark://hadoop001:7077 --class demo.WorldCount /data/sparkDemo.jar hdfs://hadoop001:9000/wordCount.txt hdfs://hadoop001:9000/wordCount
 */
public class Java7WordCount {
    public static void main(String[] args) {
        //创建spark配置，设置应用程序名字,设置本地运行以及默认的分区数,生成几个task(最后有几个文件)
        //SparkConf conf = new SparkConf().setAppName("Java7WordCount")
        SparkConf conf = new SparkConf().setAppName("Java7WordCount").setMaster("local[1]");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        //JavaRDD<String> lines = jsc.textFile(args[0]);

        List<String> list = Arrays.asList(new String[]{"hello i am tom", "tom my name is lilei", "hi lilei i am lucy"});
        JavaRDD<String> lines = jsc.parallelize(list);

        //切分压平(单个数使用JavaRDD)
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //将单词和一组合在一起(多个数的使用JavaPairRDD)
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //调换顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                return tp.swap();
            }
        });
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调换顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tp) throws Exception {
                System.out.println(tp);
            }
        });
        //将数据保存到hdfs
        //result.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();
    }
}
