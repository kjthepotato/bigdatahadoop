import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockChange {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text dateKey = new Text();
        private DoubleWritable closePrice = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Date")) {
                return; // skip header
            }
            String[] parts = line.split(",");
            if (parts.length >= 5) {
                try {
                    dateKey.set(parts[0]);
                    closePrice.set(Double.parseDouble(parts[4]));
                    context.write(dateKey, closePrice);
                } catch (NumberFormatException e) {
                    // skip bad data
                }
            }
        }
    }

    public static class StockReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Change");
        job.setJarByClass(StockChange.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}