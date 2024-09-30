import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class MaxMinTemp {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {

        public static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (!line.isEmpty()) {
                String date = line.substring(6, 14);
                float maxTemp = Float.parseFloat(line.substring(39, 45).trim());
                float minTemp = Float.parseFloat(line.substring(47, 53).trim());

                if (maxTemp > 30.0) {
                    context.write(new Text("Hot Day: " + date), new Text(String.valueOf(maxTemp)));
                }
                if (minTemp < 15.0) {
                    context.write(new Text("Cold Day: " + date), new Text(String.valueOf(minTemp)));
                }
            }
        }
    }

    public static class TempReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            String temperature = values.next().toString();
            context.write(key, new Text(temperature));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Min Temperature");

        job.setJarByClass(MaxMinTemp.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(args[0]); // Replace with actual input path
        Path outputPath = new Path(args[1]); // Replace with actual output path

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
