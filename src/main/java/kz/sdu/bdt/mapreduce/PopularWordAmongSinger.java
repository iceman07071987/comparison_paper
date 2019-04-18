package kz.sdu.bdt.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

import static kz.sdu.bdt.StringUtils.*;

public class PopularWordAmongSinger extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            for (String token : clearString(fields[2]).split(" ")) {
                if (!STOP_WORDS.contains(token) && token.length() > 3) {
                    word.set(fields[0] + "|" + token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IdentityMapper extends Mapper<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] vals = key.toString().split("\\|");
            k.set(vals[0]);
            v.set(vals[1]+":"+value.toString());
            context.write(k, v);
        }
    }

    public static class MaxReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            int maxValue = -1;
            String word = "";

            while (it.hasNext()) {
                String[] val = it.next().toString().split(":");
                int count = Integer.parseInt(val[1]);

                if (count > maxValue) {
                    maxValue = count;
                    word = val[0];
                }
            }

            context.write(key, new Text(word+"("+maxValue+")"));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Job started at : " + new Date());
        long millis = System.currentTimeMillis();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Popular words count <in> [<in>...] <out>");
            System.exit(2);
        }
        int res = ToolRunner.run(conf, new PopularWordAmongSinger(), args);
        System.out.println("Job completed in " + (System.currentTimeMillis() - millis) + "ms.");
        System.exit(res);

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "PopularWordAmongSinger");
        job.setJarByClass(PopularWordAmongSinger.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < strings.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(strings[i]));
        }

        String outPath = strings[strings.length - 1];
        FileOutputFormat.setOutputPath(job,
                new Path(outPath, "out1"));

        if (!job.waitForCompletion(true)) {
            return 1;
        }

        Job job2 = Job.getInstance(conf, "max");
        job2.setJarByClass(PopularWordAmongSinger.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(MaxReducer.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(outPath, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(outPath, "out2"));
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
