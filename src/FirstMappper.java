import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FirstMappper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");  //以tab键为分隔符
        if (line.length >= 2) {
            String id = line[0].trim();     //微博的ID
            String content = line[1].trim();  //微博的内容
            StringReader sr = new StringReader(content);
            IKSegmenter iks = new IKSegmenter(sr, true);  //使用
            Lexeme lexeme = null;
            while ((lexeme = iks.next()) != null) {
                String word = lexeme.getLexemeText();   //word就是分完的每个词
                context.write(new Text(word + "_" + id), new IntWritable(1));//
            }
            sr.close();
            context.write(new Text("count"), new IntWritable(1));//
        } else {
            System.err.println("error:" + value.toString() + "-----------------------");
        }
    }
}


