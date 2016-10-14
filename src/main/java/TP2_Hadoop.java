import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Gib on 12/10/2016.
 */
public class TP2_Hadoop
{
    //default mapper
    public static class defaultMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    //task 1 : count number of names by origin
    public static class customMapper1
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //get row
            String myRow = new String(value.toString());
            //get cols
            String[] tab = myRow.split(";");
            //get several origins
            String[] tab2 = tab[2].replaceAll(", ",",").split(",");
            //send each origin
            for(String myWord : tab2)
            {
                word.set(myWord);
                context.write(word, one);
            }
        }
    }

    //task 2 : count number of names by number of origins
    public static class customMapper2
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //get row
            String myRow = new String(value.toString());
            //get cols
            String[] tab = myRow.split(";");
            //get several origins
            String[] tab2 = tab[2].replaceAll(", ",",").split(",");
            //send number of origins
            if(tab2[0].equals(""))
            {
                word.set("0");
            }
            else
            {
                word.set(Integer.toString(tab2.length));
            }
            context.write(word, one);
        }
    }

    //task 3 : count percentage of names by gender
    public static class customMapper3
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //get row
            String myRow = new String(value.toString());
            //get cols
            String[] tab = myRow.split(";");
            //get separate genders
            String[] tab2 = tab[1].replaceAll(", ",",").split(",");

            //send gender(s)
            for(int i=0; i<tab2.length; i++)
            {
                word.set(tab2[i]);
                context.write(word, one);
            }
        }
    }

    //default reducer
    public static class defaultReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    //custom reducer for task 3 : count percentage of names by gender w/ global variables because why not
    public static int gender1, gender2, cpt=1, total;
    public static int gender1percent, gender2percent;
    public static String gender1str, gender2str;
    public static class customReducer3
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);

            //set global variables with keys and values
            if(TP2_Hadoop.cpt==1)
            {
                TP2_Hadoop.gender1 = sum;
                TP2_Hadoop.gender1str = String.valueOf(key);
                TP2_Hadoop.cpt++;
            }
            else
            {
                TP2_Hadoop.gender2 = sum;
                TP2_Hadoop.gender2str = String.valueOf(key);
                TP2_Hadoop.total = TP2_Hadoop.gender1 + TP2_Hadoop.gender2;

                //do percentage calculation
                TP2_Hadoop.gender1percent = 1000*TP2_Hadoop.gender1/TP2_Hadoop.total;
                TP2_Hadoop.gender2percent = 1000*TP2_Hadoop.gender2/TP2_Hadoop.total;

                //round to superior value in specific cases
                if(TP2_Hadoop.gender1percent%10>=5)
                {
                    gender1percent+=5;
                }
                if(TP2_Hadoop.gender2percent%10>=5)
                {
                    gender2percent+=5;
                }
                gender1percent = gender1percent/10;
                gender2percent = gender2percent/10;

                //send first gender with %
                key.set(TP2_Hadoop.gender1str);
                result.set(gender1percent);
                context.write(key, result);

                //send second gender with %
                key.set(TP2_Hadoop.gender2str);
                result.set(gender2percent);
                context.write(key, result);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TP2_Hadoop.class);

        //mapper name here
        job.setMapperClass(customMapper3.class);

        //combiner name here
        job.setCombinerClass(defaultReducer.class);

        //reducer name here
        job.setReducerClass(customReducer3.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
