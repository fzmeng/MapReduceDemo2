package hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TableJoin extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: TableJoin <table1> <table2> <output>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(1);
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        Path output = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(TableJoin.class);
        job.setMapperClass(TableJoinMapper.class);
        job.setReducerClass(TableJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TableJoin(), args);
        System.exit(res);
    }


}

class TableJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");
        String id = row[0];
        outKey.set(id);

        FileSplit split = (FileSplit)context.getInputSplit();
        String tableFileName = split.getPath().getName();
        if (tableFileName.equals("person.csv")) {
            outValue.set(TableUtils.PERSON_TABLE_PREFIX + ":"
                    + TableUtils.makeString(row[1], row[2], row[3], row[4]));
            context.write(outKey, outValue);
        } else if (tableFileName.equals("score.csv")) {
            outValue.set(TableUtils.SCORE_TABLE_PREFIX + ":"
                    + TableUtils.makeString(row[1]));
            context.write(outKey, outValue);
        }
    }

}

class TableJoinReducer extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String id = key.toString();
        outKey.set(id);
        List<String> t1Values = new LinkedList<String>();
        List<String> t2Values = new LinkedList<String>();
        for (Text value: values) {
            String[] arr = value.toString().split(":");
            if (arr[0].equals(TableUtils.PERSON_TABLE_PREFIX)) {
                t1Values.add(TableUtils.makeString(arr[1]));
            } else if (arr[0].equals(TableUtils.SCORE_TABLE_PREFIX)) {
                t2Values.add(TableUtils.makeString(arr[1]));
            }
        }
        for (String v1: t1Values) {
            for (String v2: t2Values) {
                outValue.set(v1 + "," + v2);
                context.write(outKey, outValue);
            }
        }
    }
}