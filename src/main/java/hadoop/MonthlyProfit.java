package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable; // (不再需要，但保留 import 也可以)
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MonthlyProfit {

    /**
     * Mapper：
     * 输入：(offset, "1 ls 2850 100")
     * 处理：
     * 1. 分割字符串: ["1", "ls", "2850", "100"]
     * 2. 提取: month="1", person="ls", income=2850, expense=100
     * 3. 计算: profit = 2850 - 100 = 2750
     * 4. 组合 Value: "1\t2750"
     * 输出：(key, value) -> (Text("ls"), Text("1\t2750"))
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            // 按一个或多个空白字符分割
            String[] parts = line.split("\\s+");

            if (parts.length != 4) {
                // 格式不正确（需要 月份 姓名 收入 支出）
                System.err.println("Skipping invalid line: " + line);
                return;
            }

            try {
                String month = parts[0];
                String person = parts[1];
                int income = Integer.parseInt(parts[2]);
                int expense = Integer.parseInt(parts[3]);

                int profit = income - expense;

                outKey.set(person);
                outValue.set(month + "\t" + profit); // Value 格式: "月份\t利润"

                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid number in line: " + line);
            }
        }
    }

    /**
     * Reducer：
     * 因为我们设置了3个Reducer，Hadoop会把同一个Key(人名)的所有数据发到同一个Reducer。
     * Reducer 不需要求和，只需要将 (Key, Value) 原样输出。
     * * 输入：(Text("ls"), [Text("1\t2750"), Text("2\t3366"), Text("3\t4232")])
     * 输出：
     * (Text("ls"), Text("1\t2750"))
     * (Text("ls"), Text("2\t3366"))
     * (Text("ls"), Text("3\t4232"))
     * ... (这些会全部写入同一个 part-r-xxxxx 文件)
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 直接遍历 values 并将 (key, value) 写出
            for (Text val : values) {
                context.write(key, val);
            }
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
        //    !! 假设 profit.txt 文件已位于 HDFS 的 /txt/ 目录下
        Path INPUT  = new Path("/txt/profit.txt");
        Path OUTPUT = new Path("/monthly_profit"); // 使用新的输出目录

        Job job = Job.getInstance(conf, "Monthly Profit"); // 更新 Job 名称
        job.setJarByClass(MonthlyProfit.class);

        job.setMapperClass(Map.class);
        // job.setCombinerClass(); // 此处不需要 Combiner
        job.setReducerClass(Reduce.class);

        // 设置 Mapper 和 Reducer 的最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // *** 关键：按要求设置3个Reducer ***
        job.setNumReduceTasks(3); // 这将产生3个输出文件 (part-r-00000, 00001, 00002)

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

        // 打印结果到 IDEA 控制台 (现在会打印3个文件的内容)
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