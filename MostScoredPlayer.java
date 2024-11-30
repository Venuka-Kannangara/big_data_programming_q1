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

package org.iit.mapreduce.patterns.mostscoredplayer;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.iit.mapreduce.patterns.mostscoredplayer.MostScoredPlayer;


import java.io.IOException;





public class MostScoredPlayer {

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
    
    public static class MostScoredPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Define the variables 
        String Player_Name = null;
        String Description1 = null;
        String Description2 = null;
        int Score = 0;
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Reading the data from the CSV file
            String[] fields = value.toString().split(",");
    
            // Ensure the row has enough columns
            if (fields.length <= VISITORDESCRIPTION) {
                return; // Skip if there are not enough fields
            }
    
            // Either the Home description or Visitor description shouldn't be empty
            Description1 = fields[HOMEDESCRIPTION].trim();
            Description2 = fields[VISITORDESCRIPTION].trim();
    
            // Check and handle the Home description (Player 1)
            if (!Description1.isEmpty() && Description1.contains("PTS")) {
                // Store Player Name for player 1
                Player_Name = fields[PLAYER1_NAME].trim();
    
                // Extract the Score from the description for player 1
                String scoreStr = Description1.replaceAll(".*\\((\\d+) PTS\\).*", "$1");
    
                // Check if the score is non-empty and valid
                if (!scoreStr.isEmpty()) {
                    try {
                        Score = Integer.parseInt(scoreStr);
                        context.write(new Text(Player_Name), new IntWritable(Score));
                    } catch (NumberFormatException e) {
                        // Handle invalid score format (this will skip the invalid record)
                        System.err.println("Invalid score format for " + Player_Name + ": " + scoreStr);
                    }
                }
            }
    
            // Check and handle the Visitor description (Player 2)
            if (!Description2.isEmpty() && Description2.contains("PTS")) {
                // Store Player Name for player 2
                Player_Name = fields[PLAYER2_NAME].trim();
    
                // Extract the Score from the description for player 2
                String scoreStr = Description2.replaceAll(".*\\((\\d+) PTS\\).*", "$1");
    
                // Check if the score is non-empty and valid
                if (!scoreStr.isEmpty()) {
                    try {
                        Score = Integer.parseInt(scoreStr);
                        context.write(new Text(Player_Name), new IntWritable(Score));
                    } catch (NumberFormatException e) {
                        // Handle invalid score format (this will skip the invalid record)
                        System.err.println("Invalid score format for " + Player_Name + ": " + scoreStr);
                    }
                }
            }
        }
    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    
    public static class MostScoredPlayerReducer extends
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
    public static class TheMostScoredPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable score = new IntWritable();
        private final Text playerName = new Text();
    
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each line of input is in the form of: PlayerName\tTotalScore
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                playerName.set(fields[0].trim());  // Player name
                try {
                    score.set(Integer.parseInt(fields[1].trim()));  // Total score
                    context.write(playerName, score);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid score format: " + fields[1]);
                }
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class TheMostScoredPlayerReducer extends Reducer<Text, IntWritable, Text, Text> {
        private int maxScore = 0;
        private String topPlayer = "";
    
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            // Sum the scores for the player (though there should only be one score for each player)
            for (IntWritable val : values) {
                totalScore += val.get();
            }
    
            // Check if this player has the highest score
            if (totalScore > maxScore) {
                maxScore = totalScore;
                topPlayer = key.toString();
            }
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!topPlayer.isEmpty()) {
                // Emit the player with the highest score
                context.write(new Text(topPlayer), new Text("scored " + maxScore + " points in the full tournament."));
            }
        }
    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Most Scored Player <input path> <temp path> <output path>");
            System.exit(-1);
        }

        // Configurations and Job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Preprocessing of input Data | Mapreduce 1");
        job1.setJarByClass(MostScoredPlayer.class);
        job1.setMapperClass(MostScoredPlayerMapper.class);
        job1.setReducerClass(MostScoredPlayerReducer.class);
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
        job2.setJarByClass(MostScoredPlayer.class);
        job2.setMapperClass(TheMostScoredPlayerMapper.class);
        job2.setReducerClass(TheMostScoredPlayerReducer.class);
        job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Input is the output of Job 1
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Wait for Job 2 to complete
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
    }
}
