package hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapJoin extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MapJoin <small_table> <large_table> <output>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(1);
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        Path output = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(MapJoin.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapJoin(), args);
        System.exit(res);
    }
}

class MapJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Map<String, List<String>> smallTable;
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void setup(Context context) throws IOException {
        smallTable = new HashMap<String, List<String>>();
        URI[] uris = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        for (URI uri: uris) {
            Path p = new Path(uri.getPath());
            if (p.getName().equals("score.csv")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] row = line.split(",");
                    List<String> list = smallTable.get(row[0]);
                    if (list == null) {
                        list = new LinkedList<String>();
                    }
                    list.add(row[1]);
                    smallTable.put(row[0], list);
                }
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");
        outKey.set(row[0]);
        List<String> smallTableValues = smallTable.get(row[0]);
        if (smallTableValues != null) {
            for (String v: smallTableValues) {
                outValue.set(v + "," + TableUtils.makeString(row[1], row[2], row[3], row[4]));
                context.write(outKey, outValue);
            }
        }
    }
}
