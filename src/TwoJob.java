import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJob {

    public static void mainJob() {
        Configuration config = new Configuration();

        try {
            Job job = Job.getInstance(config, "weibo2");
            job.setJarByClass(TwoJob.class);
            //设置map任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(TwoMapper.class);
            job.setCombinerClass(TwoReducer.class);
            job.setReducerClass(TwoReducer.class);
            //设置任务运行时，数据的输入输出目录，这里的输入数据是上一个mapreduce的输出
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_OUTPUT1));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT2));
            if (job.waitForCompletion(true)) {
                System.out.println("TwoJob-执行完毕");
                LastJob.mainJob();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

