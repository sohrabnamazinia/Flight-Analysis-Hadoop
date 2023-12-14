import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class TaxiAverageTime
{
	public static TreeSet<ComparableOutput> highestOutput = new TreeSet<>();
	public static TreeSet<ComparableOutput> lowestOutput = new TreeSet<>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> 
    {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
			String[] elements = value.toString().split(",");
			String originAirport = elements[16].trim();
			String departureAirport = elements[17].trim();
			String  inTime = elements[19].trim();
			String  outTime = elements[20].trim();
			// Just to check if it is an integer and not NA
			if(isInt(outTime))
            {
				int taxiOut = Integer.parseInt(outTime);
				context.write(new Text(departureAirport), new LongWritable(taxiOut));
			}  
			// Just to check if it is an integer and not NA
			if(isInt(inTime))
            {
				int taxiIn = Integer.parseInt(inTime);
				context.write(new Text(originAirport), new LongWritable(taxiIn));
			}
		}
		public static boolean isInt(String s) 
        {
			try
			{
				Integer.parseInt(s);
				return true;
			}
			catch (NumberFormatException ex)
			{
				return false;
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> 
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
		{
			long count = 0;
			long totalTime = 0;
			for (LongWritable value: values) 
			{
				totalTime = totalTime + value.get();
				count++;
			}
			double average = totalTime / count;
			highestOutput.add(new ComparableOutput(average, key.toString()));
			lowestOutput.add(new ComparableOutput(average, key.toString()));
			if (highestOutput.size() >= 4) 
			{
				highestOutput.pollLast();
			}
			if (lowestOutput.size() >= 4) 
			{
				lowestOutput.pollFirst();
			}
			context.write(key, new Text(Double.toString(average)));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(TaxiAverageTime.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);

		File fileHighest = new File(args[1] + "/Highest_3.txt");
		fileHighest.createNewFile();
		FileWriter fileHighestWriter = new FileWriter(fileHighest);
		for (ComparableOutput temp : highestOutput) 
		{
			fileHighestWriter.write("Airport: " + temp.airport + ", among the highest average taxi times: " + temp.average + "\n");
		}
		fileHighestWriter.close();
		File fileLowest = new File(args[1] + "/Lowest_3.txt");
		fileLowest.createNewFile();
		FileWriter fileLowestWriter = new FileWriter(fileLowest);
		for (ComparableOutput now : lowestOutput) 
		{
			fileLowestWriter.write("Airport: " + now.airport + ", among the lowest average taxt times -> " + now.average + "\n");
		}
		fileLowestWriter.close();
	}

	public static class ComparableOutput implements Comparable<ComparableOutput> 
	{
		double average;
		String airport;

		ComparableOutput(double average, String key) 
		{
			this.average = average;
			this.airport = key;
		}

		@Override
		public int compareTo(ComparableOutput other) 
		{
			if(this.average <= other.average)
			{
				return 1;
			} 
			else 
			{
				return -1;
			}
		}
	}
}