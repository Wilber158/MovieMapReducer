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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestYearForMovieGenre {

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

            // If user does not input century in args calculate which year has most genre for type.
            if (targetCentury == null) {
            	// If column 8 is not \N then we continue calculation because it has genre. Some types do not. (e.g. tvSeries)
                if (cols[1].equals(targetType) && !cols[8].equals("\\N")) {
                	// Some rows can have up to 3 genres and separated by comma.
                    String[] genres = cols[8].split(",");
                    
                    // We look through genres[] to see if it has targetGenre.
                    for (String genre : genres) {
                        if (genre.equalsIgnoreCase(targetGenre)) {
                            isGenre = one;
                            break;
                        }
                    }
                    // Check if year column is not "\N"
                    if (!cols[5].equals("\\N")) {
                    	// Key Value [year, 0] if the line row is targetType (e.g. movie, short), but not the targetGenre. [year, 1] if row is a movie and is targetGenre
			// We will not calculate other types and only the targetType.
                        context.write(new Text(cols[5]), isGenre);
                    }
                }
             // If user did enter a century.
            } else {
            	// Century to year group calculation. (e.g. 21st century = (21 - 1) * 100 = 2000)
                Integer century = (targetCentury - 1) * 100;
                
                // Check if movie type is the same, has a year, is in the targetCentury, and if genres is not empty.
                if (cols[1].equals(targetType) && !cols[5].equals("\\N") && Integer.parseInt(cols[5]) >= century && Integer.parseInt(cols[5]) < (targetCentury * 100) && !cols[8].equals("\\N")) {
                	// Some rows can have up to 3 genres and separated by comma.
                    String[] genres = cols[8].split(",");
                    
                    // We look through them all to see if it has targetGenre.
                    for (String genre : genres) {
                        if (genre.equalsIgnoreCase(targetGenre)) {
                            isGenre = one;
                            break;
                        }
                    }
			// Key Value [year, 0] if the line row is targetType (e.g. movie, short), but not the targetGenre. [year, 1] if row is a movie, is targetGenre and in targetCentury
			// We will not calculate other types and only the targetType.
			context.write(new Text(cols[5]), isGenre);
                }
            }
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
            System.err.println("Usage: hadoop jar yourjarfile.jar BestYearForMovieGenre input output targetType targetGenre [century] [-s or sort]");
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
        job.setJarByClass(BestYearForMovieGenre.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobSuccess = job.waitForCompletion(true);

        if (jobSuccess && wantsSorting) {
            Path outputFile = new Path(args[1] + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);

            List<YearCount> records = new ArrayList<>();

            // Read original output file from HDFS
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

            // Overwrite the existing file
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile, true)))) {
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
