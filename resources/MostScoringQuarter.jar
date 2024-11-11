import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MostScoringQuarterJob {

  public class MostScoringQuarterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text teamQuarterKey = new Text();
    private IntWritable points = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the line to extract relevant fields
        String[] fields = value.toString().split("\t");  // Assuming TSV format

        // Extract relevant fields
        String period = fields[4];
        String score = fields[17]; // Format X-Y, where X is home and Y is visitor score
        String homeDesc = fields[2];
        String visitorDesc = fields[18];

        String team;
        int pointsScored;

        // Determine if the event was scored by home or visitor team
        if (!homeDesc.isEmpty()) {
            team = fields[7]; // Home team abbreviation
            pointsScored = extractPointsScored(score, true); // Method to get score difference as integer for home
        } else if (!visitorDesc.isEmpty()) {
            team = fields[14]; // Visitor team abbreviation
            pointsScored = extractPointsScored(score, false); // Method to get score difference as integer for visitor
        } else {
            return; // No scoring event
        }

        // Set up key as team_quarter, e.g., "LAL_Q1"
        teamQuarterKey.set(team + "_Q" + period);
        points.set(pointsScored);

        context.write(teamQuarterKey, points);
    }

    // Helper method to calculate points scored
    private int extractPointsScored(String score, boolean isHome) {
        String[] scores = score.split("-");
        int homeScore = Integer.parseInt(scores[0]);
        int visitorScore = Integer.parseInt(scores[1]);

        // Calculate the change in score for the respective team
        if (isHome) {
            return homeScore - lastHomeScore; // Assume lastHomeScore is tracked and updated between records
        } else {
            return visitorScore - lastVisitorScore; // Assume lastVisitorScore is tracked and updated
        }
    }
}

public class MostScoringQuarterReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Map<String, Integer> quarterScoreMap = new HashMap<>();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalScore = 0;
        for (IntWritable val : values) {
            totalScore += val.get();
        }

        // Store total scores in a map for later use to identify the highest scoring quarter
        quarterScoreMap.put(key.toString(), totalScore);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // After all key-values processed, determine max quarter score per team
        Map<String, String> teamMaxQuarterMap = new HashMap<>();
        Map<String, Integer> teamMaxScoreMap = new HashMap<>();

        for (Map.Entry<String, Integer> entry : quarterScoreMap.entrySet()) {
            String[] teamQuarter = entry.getKey().split("_Q");
            String team = teamQuarter[0];
            String quarter = "Q" + teamQuarter[1];
            int score = entry.getValue();

            // Update max score for each team
            if (!teamMaxScoreMap.containsKey(team) || score > teamMaxScoreMap.get(team)) {
                teamMaxScoreMap.put(team, score);
                teamMaxQuarterMap.put(team, quarter);
            }
        }

        // Write final output as team and highest scoring quarter
        for (Map.Entry<String, String> entry : teamMaxQuarterMap.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
    }
}

    public static void main(String[] args) throws Exception {
        // Check input and output arguments
        if (args.length != 2) {
            System.err.println("Usage: MostScoringQuarterJob <input path> <output path>");
            System.exit(-1);
        }

        // Create configuration
        Configuration conf = new Configuration();

        // Create the job
        Job job = Job.getInstance(conf, "Most Scoring Quarter by Team");
        job.setJarByClass(MostScoringQuarterJob.class);

        // Set mapper and reducer classes
        job.setMapperClass(MostScoringQuarterMapper.class);
        job.setReducerClass(MostScoringQuarterReducer.class);

        // Specify the output key and value types produced by mapper and reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths from the arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion and exit based on success or failure
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
