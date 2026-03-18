import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai2 
{
    // (MovieID, Genre)
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text movieIdKey = new Text();
        private Text movieGenresValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String movieId = parts[0].trim();
                String movieGenres = parts[2].trim();

                movieIdKey.set(movieId);
                movieGenresValue.set("Genres: " + movieGenres);
                context.write(movieIdKey, movieGenresValue);
            }
        }   
    }

    // (MovieID, Rate)
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                movieIdKey.set(movieId);
                ratingValue.set("Rate: " + rating);
                context.write(movieIdKey, ratingValue);
            }
        }
    }

    // return (Genre, Rate)
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String movieGenres = ""; 
            ArrayList<String> ratings = new ArrayList<>();

            for (Text val : values) 
            {
                String value = val.toString();
                
                if (value.startsWith("Genres: ")) {
                    movieGenres = value.replace("Genres: ", "").trim();
                } else if (value.startsWith("Rate: ")) {
                    ratings.add(value.replace("Rate: ", "").trim());
                }
            }

            if (!movieGenres.isEmpty()) 
            {
                String[] genres = movieGenres.split("\\|");

                for (String genre : genres) {
                    outputKey.set(genre.trim());
                    
                    for (String rate : ratings) {
                        outputValue.set(rate); 
                        context.write(outputKey, outputValue); 
                    }
                }
            }
        }
    }
    
    // Job 2: Avg Rate per Genre
    public static class Job2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text genreKey = new Text();
        private Text rateValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                genreKey.set(parts[0].trim());
                rateValue.set(parts[1].trim());
                context.write(genreKey, rateValue);
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (Text val : values) {
                double rate = Double.parseDouble(val.toString().trim());
                sum += rate;
                count++;
            }

            if (count > 0) {
                double avg = sum / count;
                
                outputKey.set(String.format("%s ", key));
                outputValue.set(String.format("Avg: %.2f, Count: %d", avg, count));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
       Configuration conf = new Configuration();

       Job job1 = Job.getInstance(conf, "Concat Movie vs Rating");

       job1.setJarByClass(bai2.class);

       MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, RatingMapper.class);
       
       job1.setReducerClass(GenreReducer.class);

       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(Text.class);
       FileOutputFormat.setOutputPath(job1, new Path(args[3]));

       if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 fail!");
            System.exit(1);
       }

        // ------ CẤU HÌNH JOB 2 ------
        Job job2 = Job.getInstance(conf, "Job 2: Avg Rate per Genre");
        job2.setJarByClass(bai2.class);

        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(RatingReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        boolean success = job2.waitForCompletion(true);

        if (success) {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(args[3]), true);
        }

        System.exit(success ? 0 : 1);
    }
}
