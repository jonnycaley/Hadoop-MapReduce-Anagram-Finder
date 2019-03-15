import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramFinder {

	public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text sortedWord = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitter = value.toString().split("[-.,:/]"); //Split the word at - . , characters if possible

			for (int x = 0; x < splitter.length; x++) { //for every string in the split array

				StringTokenizer tokenizer = new StringTokenizer(value.toString());
				
				while (tokenizer.hasMoreTokens()) { //Foreach word
					String token = tokenizer.nextToken().trim().replaceAll("[^a-zA-Z]", "").toLowerCase(); //for every word in the string
					word.set(token);
					sortedWord.set(sortString(token)); //sort the word alphabetically - this will be used as the 
					context.write(sortedWord, word);
				}
			}
		}

		protected String sortString(String input) { //function to sort string alphabetically
			char[] cs = input.toCharArray();
			Arrays.sort(cs);
			return new String(cs);
		}
		
	}
	
	

	public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();
		private Text outputKey = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				
			Set<Text> uniqueWords = new HashSet<Text>();
			int size = 0;
			StringBuilder builder = new StringBuilder();
			for (Text value : values) { //for each value
				if (uniqueWords.add(value)) { //if the value is not a duplicate of one before
					size++; //increment the size of the holding
					builder.append(value.toString()); //add the string to the builder
					builder.append(","); //add a comma
				}
			}
			builder.setLength(builder.length() - 1); //remove the comma from the end
			builder.append("]"); //add a close bracket at the end
			
			StringBuilder builderNew = new StringBuilder();
			builderNew.append("");

			if (size > 1) {
				outputValue.set(builder.toString()); // output the builder string containing all anagrams as value
				outputKey.set(builderNew.toString()); //set the output key as nothing as it is not needed
				context.write(outputKey, outputValue); 
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "["); //replace the separator with [ to give the appearance of an array
		Job job = new Job(conf, "Anagram Finder");
		job.setJarByClass(AnagramFinder.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

class AnagramFinderStringTester {

	@Test
	void test() {
		
		String[] testString = "https://helloword.com/my-test-url,here,.-".split("[-.,:\\/]");
		
		String[] answerArray = new String[] {"hsttp", "dehlloorw", "cmo","my", "estt", "lru", "eehr"};
		
		AnagramFinder finder = new AnagramFinder();
		
		for (int x = 0; x < testString.length; x++) {
			assertEquals(answerArray[x], finder.sort(testString[x].replaceAll("[^a-zA-Z]", "").toLowerCase()));
			fail("TestFail");
		}
	}

}

