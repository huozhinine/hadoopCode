package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;

public class WordCount2 {

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
        BasicConfigurator.configure();
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        final String NN = "hdfs://192.168.184.133:9000";  // 直接用 IP 指向那台能服务的 NameNode（通常 active 那台）
        final Path INPUT  = new Path(NN + "/txt/characters.txt");
        final Path OUTPUT = new Path(NN + "/charcount");

        // 不加载任何站点XML，直接手工配置
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", NN);
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        FileSystem fs = FileSystem.get(new URI(NN), conf, "hadoop");

        if (fs.exists(OUTPUT)) {
            fs.delete(OUTPUT, true);
        }

        Job job = Job.getInstance(conf, "char letter vs non-letter");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, INPUT);
        FileOutputFormat.setOutputPath(job, OUTPUT);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            fs.close();
            System.exit(1);
        }

        // 打印输出文件
        for (FileStatus st : fs.listStatus(OUTPUT)) {
            if (!st.isFile()) continue;
            if (st.getPath().getName().startsWith("part-")) {
                try (FSDataInputStream in = fs.open(st.getPath())) {
                    org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
                }
                System.out.println();
            }
        }

        fs.close();
    }

}
