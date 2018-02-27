import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.FloatWritable;


public class Youhao_Wang_Average {

   
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   		 if (otherArgs.length < 2) {
     		 System.err.println("Usage: countaverage <in> [<in>...] <out>");
    		 System.exit(2);
   		 }

		Job job = Job.getInstance(conf, "Count Average");
		job.setJarByClass(Youhao_Wang_Average.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(AverageReducer.class);

		 for (int i = 0; i < otherArgs.length - 1; ++i) {
 	         FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	        }
        job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class);
				
		job.setOutputKeyClass(Text.class);  
        //job.setOutputValueClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);  
	}


	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] lines = line.split(",");

				if(lines[3] == null || lines[3].equals("") || lines[3].equals("event")) {
					return;
				}			
				//System.out.println(lines[3]);
				String event = lines[3];
				event = event.replaceAll("\'","").replaceAll("-","");   //remove ' and -
				event = event.toLowerCase().replaceAll("[^a-zA-Z0-9]", " ");  //replace other punctuation characters with wthite space
				event = event.trim();  //remove leanding and end white space
				event = event.replaceAll("( )+", " ");    //remove multiple white space
				if(event.equals("") || event.equals(" "))	return;

				String pages = lines[18];
				outKey.set(event);
				outValue.set(pages);
				context.write(outKey, outValue);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}



	public static class AverageReducer extends Reducer<Text, Text, Text, Text>{
		private Text event = new Text();
		private Text avg = new Text();

		@Override
			protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
				int count = 0;
				int sum = 0;
				double average = 0;

				for(Text val : value) {
					sum += Integer.parseInt(val.toString());
					count ++;
				}
				average = (double) sum / count;
				//average = Math.round( (average *1000.0) / 1000.0);
				event.set(key);

				//String res = count + "\t" + String.format("%.3f", average);
				DecimalFormat f = new DecimalFormat("#.0##");
				String res = count + "\t" + f.format(average);
				avg.set(res);

				context.write(event, avg);
			}
	}
}

