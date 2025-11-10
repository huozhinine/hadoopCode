package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;

public class WordCount {

    /** Mapper：逐字符判断，letter / non_letter 各 +1 **/
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                    outKey.set("letter");
                } else {
                    outKey.set("non_letter");
                }
                context.write(outKey, ONE);
            }
        }
    }

    /** Reducer：求和 **/
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 减少 log4j 报错
        BasicConfigurator.configure();
        // 用 HDFS 上的有权限账户
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1. 加载 classpath 下的 core-site.xml / hdfs-site.xml
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");

        // 可选：为了防止 Windows ↔ Linux 内网NAT，明确一下
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        // 2. HDFS 上的输入输出路径（注意这里不再手写 IP）
        Path INPUT  = new Path("/txt/characters.txt"); // 相对 fs.defaultFS = hdfs://mycluster
        Path OUTPUT = new Path("/charcount");

        Job job = Job.getInstance(conf, "char letter vs non-letter");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1); // 单输出文件

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(OUTPUT)) {
            fs.delete(OUTPUT, true);
        }

        FileInputFormat.setInputPaths(job, INPUT);
        FileOutputFormat.setOutputPath(job, OUTPUT);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            fs.close();
            System.exit(1);
        }

        // 打印结果到 IDEA 控制台
        System.out.println("\n=== MapReduce 结果（来自 " + OUTPUT + "）===");
        for (FileStatus st : fs.listStatus(OUTPUT)) {
            if (!st.isFile()) continue;
            String name = st.getPath().getName();
            if (name.startsWith("part-")) {
                try (FSDataInputStream in = fs.open(st.getPath())) {
                    org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
                }
                System.out.println();
            }
        }

        fs.close();
    }

}
