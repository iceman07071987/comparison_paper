package kz.sdu.bdt.mapreduce;

import kz.sdu.bdt.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static kz.sdu.bdt.StringUtils.STOP_WORDS;
import static kz.sdu.bdt.StringUtils.clearString;

public class MinhashSongs extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text val = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            word.set(fields[0] + "|" + fields[1]);
            val.set(StringUtils.createSongHashes(fields[2]));
            context.write(word, val);
        }
    }

    public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Job started at : " + new Date());
        long millis = System.currentTimeMillis();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Songs min hash <in> [<in>...] <out>");
            System.exit(2);
        }
        int res = ToolRunner.run(conf, new MinhashSongs(), args);
        System.out.println("Job completed in " + (System.currentTimeMillis() - millis) + "ms.");
        System.exit(res);

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MinhashSongs");
        job.setJarByClass(MinhashSongs.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < strings.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(strings[i]));
        }

        String outPath = strings[strings.length - 1];
        FileOutputFormat.setOutputPath(job,
                new Path(outPath, "out1"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
