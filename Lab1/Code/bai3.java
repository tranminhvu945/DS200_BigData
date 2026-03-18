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

public class bai3 
{
    // Job 1: Join User vs Rating
    public static class UserMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text userIdKey = new Text();
        private Text userGenderValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length >= 3) 
            {
                String userId = parts[0].trim();
                String userGender = parts[1].trim();

                userIdKey.set(userId);
                userGenderValue.set("Gender: " + userGender);
                context.write(userIdKey, userGenderValue);
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

    public static class UserRatingReducer extends Reducer<Text, Text, Text, Text> 
    {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String userGender = "";
            ArrayList<String> ratingList = new ArrayList<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("Gender:")) {
                    userGender = value.replace("Gender: ",""); 
                } else if (value.startsWith("MovieID:")) {
                    ratingList.add(value);
                }
            }

            if (!userGender.isEmpty()) {
                for (String ratingEntry : ratingList) {
                    // Format "MovieID: 1043, Rating: 4.0"
                    String[] parts = ratingEntry.split(",");
                    String movieId = parts[0].replace("MovieID: ", "").trim();
                    String ratingScore = parts[1].replace("Rating: ", "").trim();

                    outputKey.set(movieId);
                    outputValue.set("Gender: " + userGender + ", Rating: " + ratingScore);
                    
                    context.write(outputKey, outputValue);
                    // Format (23, "Gender: Male, Rating: 4.0")
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

    public static class RatingGenderMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text movieIDKey = new Text();
        private Text ratingGenderValue = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                movieIDKey.set(parts[0].trim());
                ratingGenderValue.set(parts[1].trim());
                context.write(movieIDKey, ratingGenderValue);
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
            double sumMale = 0.0;
            int countMale = 0;
            double sumFemale = 0.0;
            int countFemale = 0;

            for (Text val : values) 
            {
                String Value = val.toString().trim();
                if (Value.startsWith("Name:")) 
                {
                    movieName = Value.replace("Name: ", "").trim();
                } 
                else if (Value.startsWith("Gender: ")) 
                {
                    try {
                        String[] parts = Value.split(",");
                        String gender = parts[0].replace("Gender: ", "").trim();
                        String ratingStr = parts[1].replace("Rating: ", "").trim();
                        double rating = Double.parseDouble(ratingStr);

                        if (gender.equalsIgnoreCase("M")) {
                            sumMale += rating;
                            countMale++;
                        } else if (gender.equalsIgnoreCase("F")) {
                            sumFemale += rating;
                            countFemale++;
                        }
                    } catch (Exception e) {}
                }
            }

            double maleAvg = countMale > 0 ? sumMale / countMale : 0.0;
            double femaleAvg = countFemale > 0 ? sumFemale / countFemale : 0.0;

            movieTitleKey.set(movieName);
            resultValue.set(String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg));

            context.write(movieTitleKey, resultValue);
        }
    }

    public static void main(String[] args) throws Exception
    {
       Configuration conf = new Configuration();

       Job job1 = Job.getInstance(conf, "Join Rating vs Users");

       job1.setJarByClass(bai3.class);

       MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
       MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, RatingMapper.class);
       
       job1.setReducerClass(UserRatingReducer.class);
       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(Text.class);
       FileOutputFormat.setOutputPath(job1, new Path(args[4]));

       if (job1.waitForCompletion(true)) 
        {
            Job job2 = Job.getInstance(conf, "Calculate Average by Gender");
            job2.setJarByClass(bai3.class);

            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MovieMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, RatingGenderMapper.class);

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