package ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class NgramMapReduce extends Configured implements Tool{
    private static int CurrentVolume = 1;

    public static enum Profiles {
        A1 ('a', 1),
        B1 ('b', 1),
        A2 ('a', 2),
        B2 ('b', 2);

        private final char profileChar;
        private final int ngramNum;

        private Profiles(char c, int n) {
            profileChar = c;
            ngramNum = n;//#TODO#: initialize Profiles class variables
        }

        public boolean equals(Profiles p) {
            if(p.equals(profileChar)){
                return true;//#TODO#: implement equals operator for instances of Profiles
            }
            return false;
        }
    }

    public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {
     
        MapWritable map = new MapWritable();
        IntWritable one = new IntWritable(1);

        private VolumeWriteable volume = new VolumeWriteable(map,one);//#TODO#: check the class definition ad update arguments
        private Text word = new Text();
        
        



        public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {
            Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1); //get profile
            volume.insertMapValue(one,one);

            String rawText = new String(bWriteable.getBytes());
            Book book = new Book(rawText, profile.ngramNum);
            StringTokenizer itr = new StringTokenizer(book.getBookBody());

            
            if(profile.profileChar == 'a') {
                if (profile.ngramNum == 1){
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken() + "\t" + book.getBookYear() + "\t" );
                        context.write(word, volume);



                    }
            }
                if(profile.ngramNum ==2){
                    String first ="";
                    String second="";
                    if(itr.hasMoreTokens()){
                        first = itr.nextToken();
                    }
                    while (itr.hasMoreTokens()) {
                        second=itr.nextToken();
                        word.set(first+ ' '+ second + "\t" + book.getBookYear());
                        volume.insertMapValue(new IntWritable(volume.hashCode()),one);
                        context.write(word, volume);
                        first = second;

                    }
                }
            }
            if(profile.profileChar == 'b') {
                if (profile.ngramNum == 1){
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken() + "\t" + book.getBookAuthor() );
                        volume.insertMapValue(new IntWritable(volume.hashCode()),one);
                        context.write(word, volume);

                    }
                }
                if(profile.ngramNum ==2){
                    String first ="";
                    String second="";
                    if(itr.hasMoreTokens()){
                        first = itr.nextToken();
                    }
                    while (itr.hasMoreTokens()) {
                        second=itr.nextToken();
                        word.set(first+ ' '+ second + "\t" + book.getBookAuthor());
                        volume.insertMapValue(new IntWritable(volume.hashCode()),one);
                        context.write(word, volume);
                        first = second;

                    }
                }
            }

        }

    }

    public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {
        private VolumeWriteable volume = new VolumeWriteable();
        private MapWritable map = new MapWritable();
        private  IntWritable volumenum = new IntWritable(1);
        public void reduce(Text key, Iterable<VolumeWriteable> values, Context context) throws IOException, InterruptedException {
           

            int sum = 0;
            for(VolumeWriteable val: values){
               sum ++;
            }
            volume.set(new MapWritable(),new IntWritable(sum));
            volume.insertMapValue(volumenum,volumenum);
        context.write(key,volume);
        }


    }

    public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
      
        Job job = Job.getInstance(conf, "ngram");

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setJarByClass(NgramMapReduce.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VolumeWriteable.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
        System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Profiles profiles[] = {Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2};
        for(Profiles p : profiles) {
            conf.setEnum("profile", p); //#TODO#: update this
            System.out.println("For profile: " + p.toString());
            if(runJob(conf, args[0], args[1]+p.toString()) != 0) //#TODO#: update this
                return 1; //error
        }
        return 0;	 //success
    }
}
