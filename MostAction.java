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

public class MostAction {

    // Mapper class
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");

            try {
                //check if row is a movie
                String titleType = cols[2].toString();
                if(!titleType.equals("movie")){
                  return;
                }
                String[] genres = cols[9].split(",");
                
                //check if action is the genre
                Boolean found = false;
                for(String genre : genres){
                  if(genre.equals("Action")){
                    found = true;
                  }
                }
                if(!found){
                  return;
                }

                //get all directors
                String[] directors = cols[12].split(",");
                String[] directorsKey = cols[10].split(",");
                //for each director create seperate key value pair
                int i = 0;
                for(String val : directorsKey){
                  String keyValue = val + "," + directors[i];
                  context.write(new Text(keyValue), new DoubleWritable(1));
                  i++;
                }
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
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    // Main function
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Directors with most action movies");
        job.setJarByClass(MostAction.class);
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