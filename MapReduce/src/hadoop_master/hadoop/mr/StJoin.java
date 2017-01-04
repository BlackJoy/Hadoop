package hadoop_master.hadoop.mr;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class StJoin {
	static class StJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text mapOutKey = new Text();
		private Text mapOutValue = new Text();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{ 
			String lineValue = value.toString();
			String strs[] = lineValue.split("\t");
			mapOutKey.set(new Text(strs[1]));
			mapOutValue.set(new Text("1"+strs[0]));
			context.write(mapOutKey, mapOutValue);
			mapOutKey.set(new Text(strs[0]));
			mapOutValue.set(new Text("2"+strs[1]));
			context.write(mapOutKey, mapOutValue);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
	}
	static class StJoinReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			String grandparents[]=new String[10]; 
			String grandchilds[]=new String[10]; 
			String tmp=new String();
			int count1=0,count2=0;
			for (Text value : values)
			{   
				tmp=value.toString();
				if(tmp.charAt(0)=='1'){
					tmp=tmp.substring(1,tmp.length());
					grandchilds[count1++]=tmp;
				}
				if(tmp.charAt(0)=='2'){
					tmp=tmp.substring(1,tmp.length());
					grandparents[count2++]=tmp;
				}
			}
			if(count1 !=0 && count2 !=0){
				for(int m=0;m<count1;m++){
					for(int n=0;n<count2;n++){
						context.write(new Text(grandchilds[m]), new Text(grandparents[n]));
					}
				}
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, StJoin.class.getSimpleName());
		job.setJarByClass(StJoin.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(StJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(StJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input4",
				"hdfs://hadoop-master:9000/opt/data/output7" };
		int status = new StJoin().run(args);
		System.exit(status);
	}
}
