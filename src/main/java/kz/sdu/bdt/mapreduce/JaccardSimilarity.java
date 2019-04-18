package kz.sdu.bdt.mapreduce;

import kz.sdu.bdt.Pair;
import kz.sdu.bdt.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class JaccardSimilarity extends Configured implements Tool {

    public static class JaccardMapper extends Mapper<Object, Text, Text, Text> {

        private final List<Pair<String, String[]>> ALL_SONGS = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] patternsURIs = Job.getInstance(context.getConfiguration()).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName();
                parseCacheFile(patternsFileName);
            }
        }

        private void parseCacheFile(String fileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    String[] vals = pattern.split("\t");
                    ALL_SONGS.add(new Pair<>(vals[0], vals[1].split(" ")));
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '");
            }
        }

        @Override
        protected void map(Object k, Text v, Context context)
                throws IOException, InterruptedException {
            String[] songs = v.toString().split("SECTION_BREAK");

            for (String s : songs) {
                String[] vals = s.split("\t");
                String key = vals[0];
                String value = vals[1];
                String song = key;

                String[] songHashes = value.split(" ");

                for (Pair<String, String[]> songAndLyric : ALL_SONGS) {
                    if (song.equals(songAndLyric.getKey())) continue;

                    if (StringUtils.jaccardSimilarity(songHashes, songAndLyric.getValue()) > 0.7) {
                        context.write(new Text(song), new Text(songAndLyric.getKey()));
                    }
                }
            }
        }
    }

    public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Job started at : " + new Date());
        long millis = System.currentTimeMillis();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Songs jaccard similarity <in> <cached_file> <out>");
            System.exit(2);
        }
        int res = ToolRunner.run(conf, new JaccardSimilarity(), args);
        System.out.println("Job completed in " + (System.currentTimeMillis() - millis) + "ms.");
        System.exit(res);

    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "JaccardSimilarity");
        job.setJarByClass(JaccardSimilarity.class);
        job.setMapperClass(JaccardMapper.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(strings[1]).toUri());

        FileInputFormat.addInputPath(job, new Path(strings[0]));

        String outPath = strings[strings.length - 1];
        FileOutputFormat.setOutputPath(job, new Path(outPath, "out1"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
