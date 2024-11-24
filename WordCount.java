/*
 * Licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.iit.mapreduce.patterns.count;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;





public class WordCount {

        private static int EVENTID = 0;
        private static int EVENTNUM = 1;
        private static int GAME_ID = 2;
        private static int HOMEDESCRIPTION = 3;
        private static int PCTIMESTRING = 4;
        private static int PERIOD = 5;
        private static int PLAYER1_ID = 6;
        private static int PLAYER1_NAME = 7;
        private static int PLAYER1_TEAM_ABBREVIATION = 8;
        private static int PLAYER1_TEAM_CITY = 9;
        private static int PLAYER1_TEAM_ID = 10;
        private static int PLAYER1_TEAM_NICKNAME = 11;
        private static int PLAYER2_ID = 12;
        private static int PLAYER2_NAME = 13;
        private static int PLAYER2_TEAM_ABBREVIATION = 14;
        private static int PLAYER2_TEAM_CITY = 15;
        private static int PLAYER2_TEAM_ID = 16;
        private static int PLAYER2_TEAM_NICKNAME = 17;
        private static int PLAYER3_ID = 18;
        private static int PLAYER3_NAME = 19;
        private static int PLAYER3_TEAM_ABBREVIATION = 20;
        private static int PLAYER3_TEAM_CITY = 21;
        private static int PLAYER3_TEAM_ID = 22;
        private static int PLAYER3_TEAM_NICKNAME = 23;
        private static int SCORE = 24;
        private static int SCOREMARGIN = 25;
        private static int VISITORDESCRIPTION = 26;
        private static int Q1 = 1;
        private static int Q2 = 1;
        private static int Q3 = 1;
        private static int Q4 = 1;
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    
    public static class TeamScoreMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Define the variables 
        String TEAM_1_Name = "null";
        String TEAM_2_Name = "null";
        String Game_ID = "null";
        String Quarter = "null";
        int Score1 = 0;
        int Score2 = 0;
        private int Team1_current_value = 0;
        private int Team2_current_value = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
    
            // Reading the data from the CSV file
            String[] fields = value.toString().split(",");
    
            // Ensure the row has enough columns
            if (fields.length <= VISITORDESCRIPTION) {
                return; // Skip if there are not enough fields
            }
            
            // Retrieving the GAME ID
            if (!fields[GAME_ID].isEmpty() && Game_ID.equals("null")) {

            }

            // Skip the header row
            if (fields[PLAYER1_TEAM_NICKNAME].trim().equalsIgnoreCase("PLAYER1_TEAM_NICKNAME")) {
                return;
            }

            // Assign team names only if they haven't been set yet
            if (TEAM_1_Name.equals("null") || TEAM_2_Name.equals("null")) {
                String team1 = fields[PLAYER1_TEAM_NICKNAME].trim();
                String team2 = fields[PLAYER2_TEAM_NICKNAME].trim();

                if (!team1.isEmpty() && !team2.isEmpty() && !team1.equals(team2)) {
                    TEAM_1_Name = team1;
                    TEAM_2_Name = team2;
                }
            }
            
            // Accessing the scores of the two teams
            String scores = fields[SCORE].trim();
            String timeStamp = fields[PCTIMESTRING].trim();
    
            // Check if the timeStamp indicates the end of a quarter
            if (timeStamp.equals("0:00")) {
                if (scores.isEmpty()) {
                    return; // Skip if the score field is empty
                }
                // Only first 4 quarters are being considered for the analysis
                int Current_Quarter = Integer.parseInt(fields[PERIOD].trim());
                if (Current_Quarter < 5){
                    // Split the score by " - "
                    String[] parts = scores.split(" - ");
                    if (parts.length == 2) { // Ensure there are two parts
                        try {
                            int current_value1= Integer.parseInt(parts[0].trim());
                            int current_value2= Integer.parseInt(parts[1].trim());
                            // Handle the first quarter differently
                            if (Current_Quarter == 1) {
                                Score1 = current_value1; // Directly use the parsed scores
                                Score2 = current_value2;
                            } else {
                                Score1 = current_value1 - Team1_current_value;
                                Score2 = current_value2 - Team2_current_value;
                            }
                            // update the stored values with current values
                            if (Current_Quarter != 4) {
                                Team1_current_value = current_value1;
                                Team2_current_value = current_value2;
                            }
                            // reset to 0, at end of each game (Note: Here only the first four quarters from each game has been cosidered.) 
                            else {
                                Team1_current_value = 0;
                                Team2_current_value = 0;    
                            }
                        } catch (NumberFormatException e) {
                            return; // Skip invalid scores
                        }
                        
                        Quarter = fields[PERIOD].trim();
                        
                    }
                }
            }
    
            // Only write if team names and quarter are valid
            if (!TEAM_1_Name.equals("null") && !TEAM_2_Name.equals("null") && !Quarter.equals("null")) {
                context.write(new Text(TEAM_1_Name + "_" + Quarter), new IntWritable(Score1));
                context.write(new Text(TEAM_2_Name + "_" + Quarter), new IntWritable(Score2));
                // Reset the team names after writing to context
                TEAM_1_Name = "null";
                TEAM_2_Name = "null";
            }
            
        }
        
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class TeamScoreReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

         public void reduce(Text key, Iterable<IntWritable> values,
                            Context context) throws IOException, InterruptedException {
             int sum = 0;
             for (IntWritable val : values) {
                 sum += val.get();
             }

             result.set(sum);
             context.write(key, result);
            }
        }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //Mapper function to extract the team name, quarter, and score.
    public static class MostScoringQuarterMapper extends Mapper<Object, Text, Text, Text> {

        private Text team = new Text();
        private Text quarterAndScore = new Text();
    
        public MostScoringQuarterMapper() {
            // Default constructor 
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return; // Skip empty lines
            }
    
            // Split input line by whitespace
            String[] parts = line.split("\\s+");
            if (parts.length != 2) {
                return; // Skip malformed lines
            }
    
            // Extract team and quarter info
            String[] teamAndQuarter = parts[0].split("_");
            if (teamAndQuarter.length != 2) {
                return; // Skip malformed data
            }
    
            String teamName = teamAndQuarter[0];
            String quarter = teamAndQuarter[1];
            String score = parts[1];
    
            // Emit the team name as key and "quarter,score" as value
            team.set(teamName);
            quarterAndScore.set(quarter + "," + score);
            context.write(team, quarterAndScore);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Reducer function to find the quarter with the maximum score for the team.
    public static class MostScoringQuarterReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();
        
        public MostScoringQuarterReducer() {
            // Default constructor 
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String mostScoringQuarter = "";
            int maxScore = Integer.MIN_VALUE;
    
            // Iterate through the values to find the most scoring quarter
            for (Text value : values) {
                String[] quarterAndScore = value.toString().split(",");
                if (quarterAndScore.length != 2) {
                    continue; // Skip malformed data
                }
    
                String quarter = quarterAndScore[0];
                int score = Integer.parseInt(quarterAndScore[1]);
    
                if (score > maxScore) {
                    maxScore = score;
                    mostScoringQuarter = quarter;
                }
            }
    
            // Emit the team and its most scoring quarter with the score
            result.set("scored most of the points in the " + mostScoringQuarter + " quarter.");
            context.write(key, result);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Most Scoring Quarter <input path> <temp path> <output path>");
            System.exit(-1);
        }

        // Configurations and Job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Preprocessing of input Data | Mapreduce 1");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TeamScoreMapper.class);
        job1.setReducerClass(TeamScoreReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        // Wait for Job 1 to complete before proceeding
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Configuration and Job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Seeking the most scoring quarter for each Team");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(MostScoringQuarterMapper.class);
        job2.setReducerClass(MostScoringQuarterReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input is the output of Job 1
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Wait for Job 2 to complete
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
