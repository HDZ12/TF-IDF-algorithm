import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override              //text就是map中计算出来的key值
    protected void reduce(Text text, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : iterable) {
            sum += intWritable.get();
        }//计算微博总条数，并进行输出
        if (text.equals("count")) {
            System.out.println(text.toString() + "==" + sum);
        }
        context.write(text, new IntWritable(sum));
    }
}

