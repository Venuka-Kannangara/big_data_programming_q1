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
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;


public class MostScoredPlayer {

    // Saving necessary column IDs to variables 
        private static int GAME_ID = 2;
        private static int HOMEDESCRIPTION = 3;
        private static int PLAYER1_NAME = 7;
        private static int PLAYER2_NAME = 13;
        private static int VISITORDESCRIPTION = 26;
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    
    public static class MostScoredPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Define the variables 
        String Player_Name = null;
        String Game_ID = null;
        String Description1 = null;
        String Description2 = null;
        int Score = 0;

        // Creating hash table to store Player and score values temporary 
        private HashMap<String, Integer> hashMap = new HashMap<>();
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Reading the data from the CSV file
            String[] fields = (value.toString() +" ").split(",");
            

            // Ensure the row has enough columns
            if (fields.length <= VISITORDESCRIPTION) {
                return; // Skip if there are not enough fields
            }
            
            Game_ID = fields[GAME_ID].trim();
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
                        String Output_string = Game_ID +"_"+ Player_Name;
                        hashMap.put(Output_string, Score);
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
                        String Output_string = Game_ID +"_"+ Player_Name;
                        hashMap.put(Output_string, Score);
                    } catch (NumberFormatException e) {
                        // Handle invalid score format (this will skip the invalid record)
                        System.err.println("Invalid score format for " + Player_Name + ": " + scoreStr);
                    }
                }
            }
        }

        // Cleanup method can be used to write the key-value pairs in hashmap to the mapper output 
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Write all the entries from the hashMap to the context
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }

    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    
    public static class MostScoredPlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            // Extract the player name from the composite key (Game_ID_PlayerName)
            String compositeKey = key.toString();
            String[] parts = compositeKey.split("_");
            
            if (parts.length < 2) {
                // Skip malformed keys
                return;
            }
            
            // Extract the PlayerName from the composite key
            String playerName = parts[1];
            
            // Sum up the scores for this player
            int totalScore = 0;
            for (IntWritable value : values) {
                totalScore += value.get();
            }
    
            // Emit the player name and total score
            context.write(new Text(playerName), new IntWritable(totalScore));
        }
    }
    

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class TheMostScoredPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input line: PlayerName,Score
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) {
                // Skip malformed lines
                return;
            }

            String playerName = fields[0].trim();
            int score;
            try {
                score = Integer.parseInt(fields[1].trim());
            } catch (NumberFormatException e) {
                // Skip invalid score values
                return;
            }

            // Emit player name and score
            context.write(new Text(playerName), new IntWritable(score));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class TheMostScoredPlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String mostScoredPlayer = null;
    private int highestScore = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            // Aggregate the total score for the player (if scores are already summed, this step isn't necessary)
            int totalScore = 0;
            for (IntWritable value : values) {
                totalScore += value.get();
            }

            // Check if this player has the highest score
            if (totalScore > highestScore) {
                highestScore = totalScore;
                mostScoredPlayer = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the player with the highest score
            if (mostScoredPlayer != null) {
                context.write(new Text(mostScoredPlayer + " has scored highest in the full tournament and the score is "), new IntWritable(highestScore));
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
