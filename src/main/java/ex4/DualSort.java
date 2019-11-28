package ex4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class DualSort {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // 使用默认的日志配置，可以在idea运行时显示日志
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DualSort");
        job.setJarByClass(DualSort.class);
        job.setMapperClass(DualSortMapper.class);
        job.setPartitionerClass(DualSortPartition.class);
        job.setReducerClass(DualSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }
}

class DualSortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable outKey = new IntWritable();
    private IntWritable outValue = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        if(stringTokenizer.hasMoreTokens()) {
            // 第一列作为 key
            outKey.set(Integer.valueOf(stringTokenizer.nextToken()));
            // 第二列作为 value
            outValue.set(Integer.valueOf(stringTokenizer.nextToken()));
            context.write(outKey, outValue);
        }
    }
}

class DualSortPartition extends HashPartitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numReduceTasks) {
        // key是 1~10 的数，key的空间大小为10，假设key是均匀分布的
        if(key.get() < 1) return 0; // not reachable
        if(key.get() > 10) return numReduceTasks - 1; // not reachable
        int size = 10 / numReduceTasks; // 每一台机器处理的key值的数量
        if(size == 0) {
            size = 10;
        }
        // 根据key划分到不同的Reducer结点
        int part = (key.get() - 1) / size;
        if(part >= numReduceTasks) {
            part = numReduceTasks - 1;
        }
        return part;
    }
}

class DualSortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable outValue = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 使用数组保存同一个key的值，然后对数组元素进行排序
        List<Integer> nums = new ArrayList();
        for(IntWritable num : values) {
            nums.add(num.get());
        }
        // 把value进行降序排序
        nums.sort((a, b) -> b - a);
        for(Integer num : nums) {
            // 将排好序的结果输出
            outValue.set(num);
            context.write(key, outValue);
        }
    }
}
