package ex4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.StringTokenizer;

public class DualSort {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // 使用默认的日志配置，可以在idea运行时显示日志
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DualSort");
        job.setJarByClass(DualSort.class);
        job.setMapperClass(DualSortMapper.class);
        job.setPartitionerClass(DualSortPartition.class);
        job.setSortComparatorClass(DualSortCompare.class);
        job.setReducerClass(DualSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }
}

class DualSortMapper extends Mapper<Object, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        if(stringTokenizer.hasMoreTokens()) {
            // 第一列和第二列共同作为 key，中间用制表符隔开，value为空字符串即可
            outKey.set(stringTokenizer.nextToken() + "\t" + stringTokenizer.nextToken());
            context.write(outKey, outValue);
        }
    }
}

class DualSortPartition extends HashPartitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        int key_first = Integer.parseInt(key.toString().split("\t")[0]);
        // key是 1~10 的数，key的空间大小为10，假设key是均匀分布的
        if(key_first < 1) return 0; // not reachable
        if(key_first > 10) return numReduceTasks - 1; // not reachable
        int size = 10 / numReduceTasks; // 每一台机器处理的key值的数量
        // 如果reducer过多size会成为0，key的空间大小为10，一台机器最少处理一个key
        if(size == 0) {
            size = 1;
        }
        // 根据key划分到不同的Reducer结点
        int part = (key_first - 1) / size;
        if(part >= numReduceTasks) {
            part = numReduceTasks - 1;
        }
        return part;
    }
}

class DualSortCompare extends WritableComparator {
    DualSortCompare(){
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String[] a_tokens = a.toString().split("\t");
        String[] b_tokens = b.toString().split("\t");
        int a_first = Integer.parseInt(a_tokens[0]);
        int b_first = Integer.parseInt(b_tokens[0]);
        if(a_first != b_first) {
            return Integer.compare(a_first, b_first);
        } else {
            int a_second = Integer.parseInt(a_tokens[1]);
            int b_second = Integer.parseInt(b_tokens[1]);
            return Integer.compare(b_second, a_second);
        }
    }
}

class DualSortReducer extends Reducer<Text, Text, Text, Text> {
    private Text outValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 对排好序的结果遍历输出即可
        for(Text num : values) {
            context.write(key, outValue);
        }
    }
}
