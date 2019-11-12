package demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * java8 可以使用lambda表达式
 * 使用规则，实现接口方法时只保留参数和返回值
 */
public class Java8WordCount {

    public static void main(String[] args) {
        //创建sparkContext
        SparkConf conf = new SparkConf().setAppName("Java8WordCount").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        //JavaRDD<String> lines = jsc.textFile(args[0]);

        List<String> list = Arrays.asList(new String[]{"hello i am tom", "tom my name is lilei", "hi lilei i am lucy"});
        JavaRDD<String> lines = jsc.parallelize(list);
        //切分压平(单个数使用JavaRDD)
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

/*
        //对比
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override  //保留参数line   中间使用  "->" 符号连接参数和返回值
           public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();//保留返回值
            }
        });
*/

        //将单词和一组合在一起(多个数的使用JavaPairRDD)
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        //调换顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调换顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        //打印到控制台
        result.foreach(tp -> {
            System.out.println(tp);
        });
        //将数据保存到hdfs
        //result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}
