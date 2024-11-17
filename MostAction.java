import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//Preprocessing such as only including type movie in the dataset was done due to memory contraints
public class MostAction {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private String targetGenre;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            targetGenre = conf.get("targetGenre", "Action");//Default:Action
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split("\t"); //TSV File

            try {
                //Check if row is movie (could be episode or short...)
                String titleType = cols[1].trim();
                if (!titleType.equalsIgnoreCase("movie")){
                    return;
                }
                if (cols[8].equals("\\N") || cols[8].trim().isEmpty()){
                    return;
                }

                //Check if movie is of target genre
                String[] genres = cols[8].split(",");
                boolean isTargetGenre = false;
                for (String genre : genres) {
                    if (genre.equalsIgnoreCase(targetGenre)) {
                        isTargetGenre = true;
                        break;
                    }
                }
                if (!isTargetGenre){
                    return;
                }

                //Get unique directorID and their name
                String[] directorsKey = cols[9].split(",");
                String[] directors = cols[11].split(",");
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
    
    /** Question requires the output to have the top in the file. Output must be sorted
     * We decided to use a priority queue that keeps the topx directors sorted
     * only these directors will be put in the output file
     * 
     * Another option was running another map reduce program to sort....
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<DirectorCount> topDirectors;
        private int topX;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.topX = conf.getInt("topX", 10); //Default: 10
            topDirectors = new PriorityQueue<>(topX, new Comparator<DirectorCount>() {
                @Override
                public int compare(DirectorCount o1, DirectorCount o2) {
                    return Integer.compare(o1.count, o2.count);
                }
            });
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            topDirectors.add(new DirectorCount(key.toString(), sum));

            if (topDirectors.size() > topX) {
                topDirectors.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //Write only the topx directos
            ArrayList<DirectorCount> directorsList = new ArrayList<>();
            while (!topDirectors.isEmpty()) {
                directorsList.add(topDirectors.poll());
            }
            Collections.reverse(directorsList);
            for (DirectorCount dc : directorsList) {
                context.write(new Text(dc.director), new IntWritable(dc.count));
            }
        }

        static class DirectorCount {
            String director;
            int count;

            DirectorCount(String director, int count) {
                this.director = director;
                this.count = count;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: MostAction <input path> <output path> <top X> <genre>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        int topX = Integer.parseInt(args[2]);
        conf.setInt("topX", topX); //Outputs top x directors
        conf.set("targetGenre", args[3]);//Custom target genre

        Job job = Job.getInstance(conf, "Most Action Movies");
        job.setJarByClass(MostAction.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
