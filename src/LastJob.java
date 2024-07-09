import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJob {

    public static void mainJob() {
        Configuration config = new Configuration();

        try {
            Job job = Job.getInstance(config, "weibo3");
            job.setJarByClass(LastJob.class);
            //将第一个job和第二个job的输出作为第三个job的输入
            //
            job.addCacheFile(new Path(Paths.TJ_OUTPUT1 + "/part-r-00003").toUri());
            //
            job.addCacheFile(new Path(Paths.TJ_OUTPUT2 + "/part-r-00000").toUri());

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // job.setMapperClass();
            job.setMapperClass(LastMapper.class);
            job.setCombinerClass(LastReduce.class);
            job.setReducerClass(LastReduce.class);

            FileInputFormat.addInputPath(job, new Path(Paths.TJ_OUTPUT1));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT3));
            if (job.waitForCompletion(true)) {
                System.out.println("LastJob-执行完毕");
                System.out.println("全部工作执行完毕");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

