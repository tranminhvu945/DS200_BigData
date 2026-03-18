import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1
{
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            if (line.isEmpty()){
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {
                String movieId = parts[0].trim();
                String movieName = parts[1].trim();

                movieIdKey.set(movieId);
                movieNameValue.set("Movie: " + movieName);
                context.write(movieIdKey, movieNameValue);
            }
            catch (Exception e) {}
        }   
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                movieIdKey.set(movieId);
                ratingValue.set("Rate: " + rating);
                context.write(movieIdKey, ratingValue);
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieName = "";
            double sum = 0.0;
            int count = 0;

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("Rate:")) {
                    String rate = value.replace("Rate: ","");
                    count++;
                    sum += Double.parseDouble(rate);
                } else {
                    movieName = value.replace("Movie: ","");
                }
            }

            double avg = sum /count;
            outputKey.set(String.format("%s ", movieName));
            outputValue.set(String.format("Average rating: %.2f (Total ratings: %d)", avg, count));
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

       Job job = Job.getInstance(conf, "MovieRating Analysis");

       job.setJarByClass(bai1.class);
       
       job.setMapperClass(MovieMapper.class);
       job.setMapperClass(RatingMapper.class);
        
       job.setReducerClass(RatingReducer.class);

       job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}