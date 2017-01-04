package hadoop_master.hadoop.wordcount;
import hadoop_master.hadoop.aves.Ave;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MyWordCount {
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text mapOutKey = new Text();
		private final static IntWritable one = new IntWritable(1);
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String strs[] = lineValue.split(" ");
			mapOutKey.set(strs[0]);
			context.write(mapOutKey, one);
		}
	}

	static class MyRducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

//	public static void main(String[] args) throws Exception {
//		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input5",
//				"hdfs://hadoop-master:9000/opt/data/outout6" };
//		Configuration configuration = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(configuration, args)
//				.getRemainingArgs();
//		if (otherArgs.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}
//		Job job = new Job(configuration, "wc");
//		job.setJarByClass(MyWordCount.class);
//		job.setMapperClass(MyMapper.class);
//		job.setReducerClass(MyRducer.class);
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(FloatWritable.class);
//		boolean isSuccess = job.waitForCompletion(true);
//		System.exit(isSuccess ? 0 : 1);
//	}
//
//}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, MyWordCount.class.getSimpleName());
		job.setJarByClass(MyWordCount.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyRducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input5",
				"hdfs://hadoop-master:9000/opt/data/output6" };
		int status = new MyWordCount().run(args);
		System.exit(status);
	}
}

