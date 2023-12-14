import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.TreeSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class FlightDelayTime 
{
	public static TreeSet<ComparableOutput> bestOutput = new TreeSet<>();
	public static TreeSet<ComparableOutput> worstOutput = new TreeSet<>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> 
    {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
			String[] elements = value.toString().split(",");
			// fetching the values
			String airline = elements[8].trim();
			String arrDelay = elements[14].trim();
			String depDelay = elements[15].trim();
			// ignroe the first header row
			if (!arrDelay.equalsIgnoreCase("ArrDelay") && !depDelay.equalsIgnoreCase("DepDelay") && !arrDelay.equalsIgnoreCase("NA") && !depDelay.equalsIgnoreCase("NA")) 
            {
				// considering delay lower than 10 minutes as on scheduled (it works)
				if (Integer.parseInt(arrDelay) <= 10 && Integer.parseInt(depDelay) <= 10) 
                {
					context.write(new Text(airline + ":ontime"), new LongWritable(1));
				}
				context.write(new Text(airline + ":all"), new LongWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> 
    {
		// define some necessary variables
		private Text currentAirline = new Text(" ");
		private DoubleWritable totalCount = new DoubleWritable();
		private DoubleWritable relCount = new DoubleWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {
			String[] tempList = key.toString().split(":");
			if (tempList[1].equals("all")) 
            {
				if (tempList[0].equals(currentAirline.toString())) 
                {
					totalCount.set(totalCount.get() + computeTotalCount(values));
				} 
                else 
                {
					currentAirline.set(tempList[0]);
					totalCount.set(computeTotalCount(values));
				}
			} 
            else 
            {
				double count = computeTotalCount(values);
				relCount.set((double) count / totalCount.get());
				Double relativeCountD = relCount.get();
				bestOutput.add(new ComparableOutput(relativeCountD,count, key.toString(), currentAirline.toString()));
				worstOutput.add(new ComparableOutput(relativeCountD,count, key.toString(), currentAirline.toString()));
				if (bestOutput.size() >= 4) 
                {
					bestOutput.pollLast();
				}
				if (worstOutput.size() >= 4) 
                {
					worstOutput.pollFirst();
				}
				context.write(key, new Text(Double.toString(relativeCountD)));
			}
		}

		private double computeTotalCount(Iterable<LongWritable> numbers) 
        {
			double count = 0;
			for (LongWritable value : numbers) 
            {
				count += value.get();
			}
			return count;
		}
	}

	public static void main(String[] args) throws Exception 
    {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(FlightDelayTime.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
		
		File bestThreeFile = new File(args[1] + "/Best_3.txt");
		bestThreeFile.createNewFile();
		FileWriter bestThreeFileWriter = new FileWriter(bestThreeFile);
		for (ComparableOutput now : bestOutput) 
        {
			bestThreeFileWriter.write("Airline: " + now.value + ", among the highest probability for being on schedule: " + now.relativeCount + "\n");
		}
		bestThreeFileWriter.close();
		File worstThreeFile = new File(args[1] + "/Worst_3.txt");
		worstThreeFile.createNewFile();
		FileWriter worstThreeFileWriter = new FileWriter(worstThreeFile);
		for (ComparableOutput temp : worstOutput) 
        {
			worstThreeFileWriter.write("Airline: " + temp.value + ", among the lowest probability for being on schedule: " + temp.relativeCount + "\n");
		}
		worstThreeFileWriter.close();
	}

	public static class ComparableOutput implements Comparable<ComparableOutput> 
    {
		String airline;
		String value;
		double relativeCount;
		double count;

		ComparableOutput(double relativeFrequency, double count, String key, String value) 
        {
			this.airline = key;
			this.value = value;
			this.relativeCount = relativeFrequency;
			this.count = count;
		}

		@Override
		public int compareTo(ComparableOutput other) 
        {
			if(this.relativeCount <= other.relativeCount)
            {
				return 1;
			}
            return -1;
		}
	}
}
