package hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 操作person.csv文件
 */
public class AgePartition extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AgePartition <input> <output>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(2);
        }

        Configuration conf = getConf();
        //conf.set(RegexMapper.GROUP,"female");
        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.setJarByClass(AgePartition.class);
        job.setMapperClass(AgeMapper.class);
        job.setPartitionerClass(AgePartitioner.class);
        job.setReducerClass(AgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(3);//reducer num  = partition num

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AgePartition(), args);
        System.exit(res);
    }
}

class AgePartitioner extends Partitioner<Text, Text> {
//14,Monica,56,female,92
    @Override
    public int getPartition(Text key, Text value, int n) {
        if (n == 0) {
            return 0;
        }
        String[] arr = value.toString().split(",");
        int age = Integer.parseInt(arr[1]);
        if (age <= 20) {
            return 0;
        } else if (age <= 50) {
            return 1 % n;
        } else {
            return 2 % n;
        }
    }
}

class AgeMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // name, age, gender, score
        String[] arr = value.toString().split(",");
        outKey.set(arr[1]); // name
        outValue.set(arr[3] + "," + arr[2] + "," + arr[4]); // gender,age,score
        context.write(outKey, outValue);
    }
}

class AgeReducer extends Reducer<Text, Text, Text, Text> {

    private volatile int maxScore = 0;
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text outKey = new Text();
        Text outValue = new Text();

        String gender="";
        int age=0;
        Map<Object,Object> resultMap = new HashMap<Object,Object>();
        String name = key.toString();
        for (Text t: values) {
            String[] arr = t.toString().split(",");//gender,age,score
            int score = Integer.parseInt(arr[2]);
            if (score > maxScore) {
                gender = arr[0];
                age = Integer.parseInt(arr[1]);
                maxScore = score;
            }
            resultMap.put(name,"age-" + age + ",gender-" + gender + ",score-" + score+",MaxScore-"+maxScore);
        }
        for(Object obj:resultMap.keySet()){
            outKey.set(String.valueOf(obj)); // name
            outValue.set(String.valueOf(resultMap.get(obj)));
            context.write(outKey, outValue);
        }
    }
}
