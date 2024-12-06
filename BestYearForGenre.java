import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.util.ArrayList; 
import java.util.Collections; 
import java.util.Comparator; 
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestYearForGenre {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);
        private String targetGenre = null;
        private Integer targetCentury = null;
        private String targetType = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	// Retrieve custom arguments from the job configuration.
            Configuration conf = context.getConfiguration();
            targetGenre = conf.get("targetGenre");
            targetType = conf.get("targetType");
            String centuryArg = conf.get("targetCentury");

            // Convert Strings into Integers IF AND ONLY IF the user added to arguments.
            if (centuryArg != null) {
                targetCentury = Integer.parseInt(centuryArg);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Every time map function gets called, value will be the next row of tsv file.
            String line = value.toString();
            String[] cols = line.split("\t");
            IntWritable isGenre = zero;

            // Check for valid type, year, genres upfront
            if (!cols[1].equals(targetType) || cols[5].equals("\\N") || cols[8].equals("\\N")) {
                return; // Invalid row
            }
            
            int year = Integer.parseInt(cols[5]);
            // Split the 3 possible genres by comma and store them in an array
            String[] genres = cols[8].split(",");
        	
            // If century is provided, ensure the year falls within it
            if (targetCentury != null) {
                int centuryStart = (targetCentury - 1) * 100;
                int centuryEnd = targetCentury * 100;
                if (year < centuryStart || year >= centuryEnd) {
                    return; // Year not in the specified century
                }
            }

	    // Search through the genres array to check if target genre is present
            for (String genre : genres) {
            	if (genre.equalsIgnoreCase(targetGenre)) {
            		isGenre = one; // Target genre is present
            	}
            }
            
            // Key Value [year, 0] if the line row is targetType (e.g. movie, short), but not the targetGenre. [year, 1] if row is a movie, is targetGenre and in targetCentury
            // We will not calculate other types and only the targetType.
            context.write(new Text(String.valueOf(year)), isGenre);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
	    if (args.length < 4) {
	        System.err.println("Usage: hadoop jar yourjarfile.jar BestYearForGenre input output targetType targetGenre [century] [-s or sort]");
	        System.exit(-1);
	    }
	    
	    Configuration conf = new Configuration();
	    conf.set("targetType", args[2]);
	    conf.set("targetGenre", args[3]);
	
	    // Handling optional arguments:
	    // If no century is given, then -s will be at args[4].
	    // If century is given, then -s will be at args[5].
	    boolean wantsSorting = false;
	    if (args.length > 4) {
	        if (args[4].equals("-s") || args[4].equals("sort")) {
	            // No century provided, just sorting
	            wantsSorting = true;
	        } else {
	            // args[4] is the century
	            conf.set("targetCentury", args[4]);
	            // If there's a fifth argument, it must be -s
	            if (args.length > 5 && (args[5].equals("-s") || args[5].equals("sort"))) {
	                wantsSorting = true;
	            }
	        }
	    }
	
	    Job job = Job.getInstance(conf, "BestYearForGenre");
	    job.setJarByClass(BestYearForGenre.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	
	    // Set the mapper/reducer output classes
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    boolean jobSuccess = job.waitForCompletion(true);
	
	    // Perform sorting if requested and job succeeded
	    if (jobSuccess && wantsSorting) {
	        Path outputFile = new Path(args[1] + "/part-r-00000");
	        FileSystem fs = FileSystem.get(conf);
	
	        List<YearCount> records = new ArrayList<>();
	
	        // Read original output file
	        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
	            String line;
	            while ((line = br.readLine()) != null) {
	                String[] parts = line.split("\t");
	                if (parts.length == 2) {
	                    String year = parts[0];
	                    int count = Integer.parseInt(parts[1]);
	                    records.add(new YearCount(year, count));
	                }
	            }
	        }
	
	        // Sort by count descending
	        Collections.sort(records, new Comparator<YearCount>() {
	            @Override
	            public int compare(YearCount o1, YearCount o2) {
	                return Integer.compare(o2.count, o1.count);
	            }
	        });
	
	        // Overwrite the existing file with sorted results
	        // Note: use `false` in create(...) to overwrite the file
	        // If needed, delete and recreate instead of append
	        fs.delete(outputFile, false);
	        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile)))) {
	            for (YearCount yc : records) {
	                bw.write(yc.year + "\t" + yc.count + "\n");
	            }
	        }
	    }
	
	    System.exit(jobSuccess ? 0 : 1);
    }

    private static class YearCount {
        String year;
        int count;
        YearCount(String year, int count) {
            this.year = year;
            this.count = count;
        }
    }
}
