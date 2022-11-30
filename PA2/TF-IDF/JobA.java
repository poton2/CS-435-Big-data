package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class JobA extends Configured implements Tool {

    public static enum DocumentsCount{
        NUMDOCS
    };


    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        System.out.println("Performing JobA1");
        Job jobA1 = Job.getInstance(conf, "A1");
        jobA1.setJarByClass(JobA.class);

        jobA1.setMapperClass(mapper1.class);
        jobA1.setReducerClass(reducer1.class);

        jobA1.setOutputKeyClass(Text.class);
        jobA1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(jobA1, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA1, new Path(args[1]));

        jobA1.waitForCompletion(true);

        Counter documentCount1 = jobA1.getCounters().findCounter(DocumentsCount.NUMDOCS);

        /****************job2***************/

        Configuration conf1 = getConf();
        System.out.println("Performing JobA2");
        Job jobA2 = Job.getInstance(conf1, "A2");
        jobA2.setJarByClass(JobA.class);

        jobA2.setMapperClass(mapper2.class);
        jobA2.setReducerClass(reducer2.class);

        jobA2.setOutputKeyClass(Text.class);
        jobA2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobA2, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobA2, new Path(args[2]));

        jobA2.waitForCompletion(true);

        /****************job3***************/

        Configuration conf2 = getConf();
        System.out.println("Performing JobA3");
        Job jobA3 = Job.getInstance(conf2,"A3");
        jobA3.setJarByClass(JobA.class);

        jobA3.setMapperClass(mapper3.class);
        jobA3.setReducerClass(reducer3.class);

        jobA3.setOutputKeyClass(Text.class);
        jobA3.setOutputValueClass(Text.class);

        jobA3.getConfiguration().setLong(documentCount1.getDisplayName(),documentCount1.getValue());

        FileInputFormat.addInputPath(jobA3, new Path(args[2]));
        FileOutputFormat.setOutputPath(jobA3, new Path(args[3]));

        return (jobA3.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JobA(), args);
        System.exit(res);
    }


    public static class mapper1 extends Mapper<Object, Text, Text, IntWritable> {


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            context.getCounter(DocumentsCount.NUMDOCS).increment(1);

            if (!value.toString().isEmpty()) {
                if (value.toString().split("<====>").length > 2) {
                    StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2].replaceAll("[^A-Za-z0-9 ]", ""));
                    String id = value.toString().split("<====>")[1];
                    while (itr.hasMoreTokens()) {
                        word.set(id + '\t' + itr.nextToken().toLowerCase());
                        context.write(word, one);  // ((DocID, Unigram) , 1 )
                    }
                }
            }
        }
    }

    public static class reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable wordcounts = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordcounts.set(sum);
            context.write(key, wordcounts);   // ((DocID, unigram), frequency)
        }
    }

    public static class mapper2 extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split("\t");
            context.write(new Text(info[0]), new Text(info[1] + "\t" + info[2])); // (DocID, (unigram , frequency))
        }
    }

    public static class reducer2 extends Reducer<Text, Text, Text, Text> {
        double max = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> strings = new ArrayList<String>();
            for (Text val1 : values) { //finding max frequency
                strings.add(val1.toString());
                double frequency = Integer.parseInt(val1.toString().split("\t")[1]);
                if (frequency > max) {
                    max = frequency;
                }
            }
            for (String val2 : strings) {
                String[] info = val2.split("\t");
                String unigram = info[0];
                double frequency = Integer.parseInt(info[1]);

                double TF = 0.5 + (0.5 + (frequency / max)); // calculate TF
                context.write(key, new Text(unigram + "\t" + TF)); //(DocID, (unigram, TF))
            }
        }
    }

    public static class mapper3 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split("\t");
            context.write(new Text(info[0]), new Text(info[1] + "\t" + info[2])); // (DocID, (unigram, TF))
        }
    }


    public static class reducer3 extends Reducer<Text,Text,Text,Text>{
        public double N;
        double IDF = 0;
        double TF_IDF = 0;

        protected void setup(Context context) throws IOException,InterruptedException{
            N = context.getConfiguration().getLong(DocumentsCount.NUMDOCS.name(),0);
        }

        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException {
            ArrayList<String> strings = new ArrayList<String>();  /** IDFi= log10(N/ni)  , TF-IDFij = Tfij * IDFij  **/
            double n = 0;

            for (Text val1 : values){
                n ++;
                strings.add(val1.toString());
            }

            for(String val2: strings){
                String[] info = val2.split("\t");
                double TF = Double.parseDouble(info[1]);
                IDF = Math.log10(N/n);
                TF_IDF = TF * IDF;
                context.write(key,new Text(info[0] + "\t" + TF_IDF)); //<docID, (unigram TF-IDF value)>
            }
        }
    }
}





