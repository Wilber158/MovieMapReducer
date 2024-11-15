import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRating {

    // Mapper class
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");

            if (cols.length < 10 || cols[5].equals("\\N") || cols[9].equals("\\N")) {
                return;
            }

            try {
                int year = Integer.parseInt(cols[5]);
                double rating = Double.parseDouble(cols[9]);

                int decade = (year / 10) * 10;
                String decadeStr = decade + "s";

                context.write(new Text(decadeStr), new DoubleWritable(rating));
            } catch (NumberFormatException e) {
                // Skip rows with invalid number formats
            }
        }
    }

    // Reducer class
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double averageRating = sum / count;
                averageRating = Math.round(averageRating * 10.0) / 10.0;
                result.set(averageRating);
                context.write(key, result);
            }
        }
    }

    // Main function
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average rating per decade");
        job.setJarByClass(AverageRating.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
