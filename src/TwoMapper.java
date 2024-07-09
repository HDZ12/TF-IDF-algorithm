import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit) context.getInputSplit();
        //map时拿到split片段所在文件的文件名
        if (!fs.getPath().getName().contains("part-r-00003")) {
            //拿到TF的统计结果
            String[] line = value.toString().trim().split("\t");
            if (line.length >= 2) {
                String[] ss = line[0].split("_");
                if (ss.length >= 2) {
                    String w = ss[0];
                    //统计DF，该词在所有微博中出现的条数，一条微博即使出现两次该词，也算一条
                    context.write(new Text(w), new IntWritable(1));
                }
            } else {
                System.out.println("error:" + value.toString() + "-------------");
            }
        }
    }
}



