import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai4
{
    // Job 1: Join User vs Rating
    public static class UserMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text userIdKey = new Text();
        private Text ageGroupValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String userId = parts[0].trim();
                int age = Integer.parseInt(parts[2].trim());

                // age group
                String ageGroup = "";
                if (age < 18) ageGroup = "0-18";
                else if (age < 35) ageGroup = "18-35";
                else if (age < 50) ageGroup = "35-50";
                else ageGroup = "50+";

                userIdKey.set(userId);
                ageGroupValue.set("AgeGroup: " + ageGroup);
                context.write(userIdKey, ageGroupValue);
            }
        }   
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text userIdKey = new Text();
        private Text movieIDratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String userID = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                userIdKey.set(userID);
                movieIDratingValue.set("MovieID: "+ movieId + ", Rating: " + rating);
                context.write(userIdKey, movieIDratingValue);
            }
        }
    }

    public static class UserAgeRatingReducer extends Reducer<Text, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = "";
            ArrayList<String> ratingList = new ArrayList<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("AgeGroup:")) {
                    ageGroup = value.replace("AgeGroup: ",""); 
                } else if (value.startsWith("MovieID:")) {
                    ratingList.add(value);
                }
            }

            if (!ageGroup.isEmpty()) {
                for (String ratingEntry : ratingList) {
                    String[] parts = ratingEntry.split(",");
                    String movieId = parts[0].replace("MovieID: ", "").trim();
                    String ratingScore = parts[1].replace("Rating: ", "").trim();

                    outputKey.set(movieId);
                    outputValue.set("AgeGroup: " + ageGroup + ", Rating: " + ratingScore);
                    
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    // Job 2: Join Movie and Calculate Average Rating
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String movieId = parts[0].trim();
                String movieName = parts[1].trim();

                movieIdKey.set(movieId);
                movieNameValue.set("Name: " + movieName);
                context.write(movieIdKey, movieNameValue);
            }
        }   
    }

    public static class RatingAgeMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text movieIDKey = new Text();
        private Text ratingAgeGroupValue = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                movieIDKey.set(parts[0].trim());
                ratingAgeGroupValue.set(parts[1].trim());
                context.write(movieIDKey, ratingAgeGroupValue);
            }
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text>
    {
        private Text movieTitleKey = new Text();
        private Text resultValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String movieName = "";
            double[] sums = new double[4];
            int[] counts = new int[4];
            String[] labels = {"0-18", "18-35", "35-50", "50+"};

            for (Text val : values) 
            {
                String Value = val.toString();
                if (Value.startsWith("Name:")) {
                    movieName = Value.replace("Name: ", "").trim();
                } else if (Value.startsWith("AgeGroup:")) 
                {
                    String[] parts = Value.split(",");
                    String agegroup = parts[0].replace("AgeGroup: ", "").trim();
                    String ratingStr = parts[1].replace("Rating: ", "").trim();
                    double rating = Double.parseDouble(ratingStr);

                    for (int i = 0; i < 4; i++) {
                        if (agegroup.equals(labels[i])) {
                            sums[i] += rating;
                            counts[i]++;
                            break;
                        }
                    }
                }
            }

            String[] results = new String[4];
            for (int i = 0; i < 4; i++) 
            {
                results[i] = (counts[i] > 0) ? String.format("%.2f", sums[i] / counts[i]) : "NA";
            }
            
            movieTitleKey.set(movieName);
            resultValue.set(String.format("0-18: %s  18-35: %s  35-50: %s  50+: %s", results[0], results[1], results[2], results[3]));

            context.write(movieTitleKey, resultValue);
        }
    }

    public static void main(String[] args) throws Exception
    {
       Configuration conf = new Configuration();

       Job job1 = Job.getInstance(conf, "Join User vs Rating");

       job1.setJarByClass(bai4.class);

       MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, RatingMapper.class);
       
       job1.setReducerClass(UserAgeRatingReducer.class);
       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(Text.class);
       FileOutputFormat.setOutputPath(job1, new Path(args[4]));

       if (job1.waitForCompletion(true)) 
        {
            Job job2 = Job.getInstance(conf, "Calculate Average by Age Group");
            job2.setJarByClass(bai4.class);

            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MovieMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, RatingAgeMapper.class);

            job2.setReducerClass(FinalReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[5]));
            
            boolean success = job2.waitForCompletion(true);

            if (success) {
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(args[4]), true);
            }

            System.exit(success ? 0 : 1);
        }
    }
}