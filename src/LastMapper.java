import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LastMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static Map<String, Integer> cmap = null; //cmap为count
    public static Map<String, Integer> df = null;
    //setup方法，表示在map之前
    protected void setup(Context context) throws IOException, InterruptedException {
        if (cmap == null || cmap.size() == 0 || df == null || df.size() == 0) {

            URI[] ss = context.getCacheFiles();
            if (ss != null) {
                for (int i = 0; i < ss.length; i++) {
                    URI uri = ss[i];
                    //判断如果该文件是part-r-00003，那就是count文件，将数据取出来放入到一个cmap中
                    if (uri.getPath().endsWith("part-r-00003")) {
                        Path path = new Path(uri.getPath());
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                        String line = br.readLine();
                        if (line.startsWith("count")) {
                            String[] ls = line.split("\t");
                            cmap = new HashMap<String, Integer>();
                            cmap.put(ls[0], Integer.parseInt(ls[1].trim()));
                        }
                        br.close();
                    } else {
                    //其他的认为是DF文件，将数据取出来放到df中
                        df = new HashMap<String, Integer>();
                        Path path = new Path(uri.getPath());
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] ls = line.split("\t");
                            df.put(ls[0], Integer.parseInt(ls[1].trim()));
                         //df这个map以单词作为key，以单词的df值作为value
                        }
                        br.close();
                    }
                }
            }
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();

        if (!fs.getPath().getName().contains("part-r-00003")) {

            String[] v = value.toString().trim().split("\t");
            if (v.length >= 2) {
                int tf = Integer.parseInt(v[1].trim());
                String[] ss = v[0].split("_");
                if (ss.length >= 2) {
                    String w = ss[0];
                    String id = ss[1];
                    //执行W = TF * Log(N/DF)计算
                    double s = tf * Math.log(cmap.get("count") / df.get(w));
                    //格式化，保留小数点后五位
                    NumberFormat nf = NumberFormat.getInstance();
                    nf.setMaximumFractionDigits(5);
                    //以 微博id+词：权重 输出
                    context.write(new Text(id), new Text(w + ":" + nf.format(s)));
                }
            } else {
                System.out.println(value.toString() + "-------------");
            }
        }
    }
}

