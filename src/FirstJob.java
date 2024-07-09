import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FirstJob {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "weibo1");
            job.setJarByClass(FirstJob.class);
            //设置map任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置reduce个数为4
            job.setNumReduceTasks(4);
            //定义一个partition表分区，哪些数据应该进入哪些分区
            job.setPartitionerClass(FirstPartition.class);
            job.setMapperClass(FirstMappper.class);
            job.setCombinerClass(FirstReducer.class);
            job.setReducerClass(FirstReducer.class);
            //设置执行任务时，数据获取的目录及数据输出的目录
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_INPUT));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT1));
            if (job.waitForCompletion(true)) {
                System.out.println("FirstJob-执行完毕");
                TwoJob.mainJob();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
