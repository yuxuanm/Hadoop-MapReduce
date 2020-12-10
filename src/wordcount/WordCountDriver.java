package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(wordcount.WordCountDriver.class);
		// specify a mapper
		job.setMapperClass(WordCountMapper.class);
		// specify a reducer
		job.setReducerClass(WordCountReducer.class);

		// specify mapper output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// specify reducer output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.18.128:9000/data/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.18.128:9000/res/wordcount"));

		if (!job.waitForCompletion(true))
			return;
	}

}
