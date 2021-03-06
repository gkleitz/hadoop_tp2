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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1 : number of names by origin");
        job.setJarByClass(TP2_Hadoop.class);

        //mapper name here
        job.setMapperClass(customMapper1.class);

        //combiner name here
        job.setCombinerClass(defaultReducer.class);

        //reducer name here
        job.setReducerClass(defaultReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
