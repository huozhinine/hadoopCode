package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; // 导入 FileSplit
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class InvertedIndex {

    /**
     * Mapper：
     * 输入：(offset, "一行文本")
     * 处理：
     * 1. 获取文件名 (例如 "file1.txt")
     * 2. 拆分单词 ("hello", "world")
     * 输出：
     * (Text("hello"), Text("file1.txt"))
     * (Text("world"), Text("file1.txt"))
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final Text word = new Text();
        private final Text fileName = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 1. 获取文件名 (来自您的提示)
            FileSplit fs = (FileSplit) context.getInputSplit();
            String name = fs.getPath().getName();
            fileName.set(name);

            // 2. 拆分单词 (使用 StringTokenizer)
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                // 3. 输出 (单词, 文件名)
                context.write(word, fileName);
            }
        }
    }

    /**
     * Reducer：
     * 输入：(Text("hello"), [Text("file1.txt"), Text("file2.txt"), Text("file1.txt")])
     * 处理：
     * 1. 使用 HashSet 去重 -> {"file1.txt", "file2.txt"}
     * 2. 拼接字符串 -> "file1.txt, file2.txt"
     * 输出：(Text("hello"), Text("file1.txt, file2.txt"))
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 1. 使用 HashSet 对文件名去重
            HashSet<String> uniqueFileNames = new HashSet<>();
            for (Text val : values) {
                uniqueFileNames.add(val.toString());
            }

            // 2. 拼接字符串
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String fileName : uniqueFileNames) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", "); // 用逗号和空格分隔
                }
                sb.append(fileName);
            }
            result.set(sb.toString());

            // 3. 输出 (单词, 文件列表)
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
        conf.set("mapreduce.framework.name", "local");

        // 可选：为了防止 Windows ↔ Linux 内网NAT，明确一下
        conf.setBoolean("dfs.client.use.datanode.hostname", true);

        // 2. HDFS 上的输入输出路径
        //    !! 假设您的文件位于 HDFS 的 /txt/invert/ 目录下
        Path INPUT  = new Path("/txt/invert/");
        Path OUTPUT = new Path("/inverted_index"); // 使用新的输出目录

        Job job = Job.getInstance(conf, "Inverted Index"); // 更新 Job 名称
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(Map.class);
        // job.setCombinerClass(); // 倒排索引不适合使用默认的 Reducer 作为 Combiner
        job.setReducerClass(Reduce.class);

        // Mapper 和 Reducer 的最终输出类型都是 (Text, Text)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置1个Reducer，将所有结果合并到单个文件中
        job.setNumReduceTasks(1);

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
                System.out.println("--- 内容来自文件: " + name + " ---");
                try (FSDataInputStream in = fs.open(st.getPath())) {
                    org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
                }
                System.out.println("\n----------------------------------\n");
            }
        }

        fs.close();
    }
}