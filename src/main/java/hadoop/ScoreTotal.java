package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ScoreTotal {

    /**
     * Mapper：
     * 输入：(offset, "Bob 90 64 92")
     * 处理：
     * 1. 分割字符串: ["Bob", "90", "64", "92"]
     * 2. 累加分数: 90 + 64 + 92 = 246
     * 输出：(key, value) -> (Text("Bob"), IntWritable(246))
     */
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            // 按一个或多个空白字符分割
            String[] parts = line.split("\\s+");

            if (parts.length < 2) {
                // 格式不正确（至少需要 姓名 + 1个分数）
                return;
            }

            String name = parts[0];
            int totalScore = 0;

            // 从第二个元素(index=1)开始遍历分数
            for (int i = 1; i < parts.length; i++) {
                try {
                    totalScore += Integer.parseInt(parts[i]);
                } catch (NumberFormatException e) {
                    // 记录错误或跳过无效分数
                    System.err.println("Skipping invalid score: " + parts[i] + " in line: " + line);
                }
            }

            outKey.set(name);
            outValue.set(totalScore);
            context.write(outKey, outValue);
        }
    }

    /**
     * Reducer：求和
     * 输入：(Text("Bob"), [246, (如果还有Bob的其它分数, 150, ...)])
     * 处理：246 + 150 + ...
     * 输出：(Text("Bob"), IntWritable(最终总和))
     * (这个 Reducer 与 WordCount 中的完全相同，因为它就是做求和)
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
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

        // *** 关键修复：切换到本地运行模式 ***
        // 通过将 "framework" 设置为 "local"，
        // MapReduce 作业将不会提交到 YARN 集群，
        // 而是直接在您 IDE 当前的 JVM 中运行。
        // 这对于调试非常有用，且不再需要 JAR 包。
//        conf.set("mapreduce.framework.name", "local");
        // *** 结束修复 ***

        // 可选：为了防止 Windows ↔ Linux 内网NAT，明确一下
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        // 2. HDFS 上的输入输出路径
        //    !! 假设 score.txt 文件已位于 HDFS 的 /txt/ 目录下
        Path INPUT  = new Path("/txt/score.txt");
        Path OUTPUT = new Path("/score_total"); // 使用新的输出目录

        Job job = Job.getInstance(conf, "Score Total"); // 更新 Job 名称

        // 在 "local" 模式下，setJarByClass 可以正常工作
        // (因为它在同一个 JVM 中)
        job.setJarByClass(ScoreTotal.class);

        // *** 关键修复 ***
        // 我们不再需要显式设置 JAR 路径
        // job.setJar("C:\\path\\to\\your\\project-jar-file.jar");
        // *** 结束修复 ***

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class); // 添加 Combiner 作为优化
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