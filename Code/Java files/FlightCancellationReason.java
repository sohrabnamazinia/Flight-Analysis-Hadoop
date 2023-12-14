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

public class FlightCancellationReason 
{
	public static TreeSet<ComparableOutput> sortedReasons = new TreeSet<>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> 
    {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
			String[] elements = value.toString().split(",");
			String cancellationReason = elements[22].trim();
			if ((!cancellationReason.equalsIgnoreCase("")) && !cancellationReason.equalsIgnoreCase("NA") && !cancellationReason.equalsIgnoreCase("CancellationCode")) 
            {
				context.write(new Text(cancellationReason), new LongWritable(1));
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> 
    {
		private LongWritable totalCount = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {
			long count = 0;
			for (LongWritable val : values) 
            {
				count += val.get();
			}
			totalCount.set(count);
			sortedReasons.add(new ComparableOutput(count, key.toString()));
			context.write(key, totalCount);
			if (sortedReasons.size() >= 2) 
            {
				sortedReasons.pollLast();
			}
		}
	}

	public static void main(String[] args) throws Exception 
    {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(FlightCancellationReason.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
		File file_reason = new File(args[1] + "/FirstReason.txt");
		file_reason.createNewFile();
		FileWriter file_reason_writer = new FileWriter(file_reason);
		for (ComparableOutput temp : sortedReasons) 
        {
			String reason_code = temp.reason;
			if(reason_code.equals("A"))
            {
				file_reason_writer.write("Cancellation Reason: Carrier, No. occurrences: " + temp.count + "\n");
			}
			else if(reason_code.equals("B"))
            {
				file_reason_writer.write("Cancellation Reason: weather, No. occurrences: " + temp.count + "\n");
			}
			else if(reason_code.equals("C"))
            {
				file_reason_writer.write("Cancellation Reason: NAS, No. occurrences: " + temp.count + "\n");
			}
			else if(reason_code.equals("D"))
            {
				file_reason_writer.write("Cancellation Reason: security, No. occurrences: " + temp.count + "\n");
			}
			else 
            {
				file_reason_writer.write("Cancellation Reason: NA");
			}
		}
		file_reason_writer.close();
	}

	public static class ComparableOutput implements Comparable<ComparableOutput> 
    {
		long count;
		String reason;

		ComparableOutput(long result2, String key) 
        {
			this.count = result2;
			this.reason = key;
		}
		@Override
		public int compareTo(ComparableOutput other) 
        {
			if (this.count <= other.count) 
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