import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostAction {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private String targetGenre = null;
        private String targetType = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Retrieve custom arguments from the job configuration
            Configuration conf = context.getConfiguration();
            targetGenre = conf.get("targetGenre");
            targetType = conf.get("targetType");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line into a string
            String line = value.toString();
            String[] cols = line.split(",");

            try {
                // Check if line has enough columns
                if (cols.length <= 12) {
                    return;
                }

                // Check if row is of the targetType (e.g., "movie")
                String titleType = cols[2].trim();
                if (!titleType.equalsIgnoreCase(targetType)) {
                    return;
                }

                // Check if genres column is not null or empty
                if (cols[9].equals("\\N") || cols[9].trim().isEmpty()) {
                    return;
                }

                // Check if targetGenre is among the genres
                String[] genres = cols[9].split(",");
                boolean isTargetGenre = false;
                for (String genre : genres) {
                    if (genre.trim().equalsIgnoreCase(targetGenre)) {
                        isTargetGenre = true;
                        break;
                    }
                }
                if (!isTargetGenre) {
                    return;
                }

                // Get director IDs and names
                if (cols[10].equals("\\N") || cols[12].equals("\\N")) {
                    return;
                }
                String[] directorsKey = cols[10].split(",");
                String[] directors = cols[12].split(",");

                // Ensure both arrays have the same length
                int minLength = Math.min(directorsKey.length, directors.length);
                for (int i = 0; i < minLength; i++) {
                    String directorId = directorsKey[i].trim();
                    String directorName = directors[i].trim();

                    if (!directorId.isEmpty() && !directorName.isEmpty()) {
                        String keyValue = directorId + "\t" + directorName;
                        context.write(new Text(keyValue), one);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MostAction <input path> <output path> <targetType> <targetGenre>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("targetType", args[2]);
        conf.set("targetGenre", args[3]);

        Job job = Job.getInstance(conf, "Directors with Most Action Movies");
        job.setJarByClass(MostAction.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class); // Optional combiner
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
