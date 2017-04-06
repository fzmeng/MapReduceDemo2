package hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NGrep extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Grep <inDir> <outDir> <regex>");
            ToolRunner.printGenericCommandUsage(System.out);
            return 2;
        }

        Configuration conf = getConf();
        conf.set(NGrepMapper.PATTERN, args[2]);
//        Path tempDir = new Path("file:///tmp/grep-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path tempDir = new Path("/tmp/grep-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        conf.set(NGrepMapper.LINE_NUMBER_DIR, tempDir.toUri().toString());

        Job grepJob = Job.getInstance(conf);
        try {
            grepJob.setJobName("grep-search");
            grepJob.setJarByClass(NGrep.class);
            grepJob.setMapperClass(NGrepMapper.class);
            grepJob.setReducerClass(NGrepReducer.class);

            FileInputFormat.setInputPaths(grepJob, args[0]);
            Path outputDir = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputDir)) {
                fs.delete(outputDir, true);
            }
            grepJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(grepJob, outputDir);
            grepJob.setOutputFormatClass(TextOutputFormat.class);
            grepJob.setOutputKeyClass(Text.class);
            grepJob.setOutputValueClass(Text.class);

            grepJob.waitForCompletion(true);
        }
        finally {
            FileSystem.get(conf).delete(tempDir, true);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NGrep(), args);
        System.exit(res);
    }
}

class NGrepMapper extends Mapper<Object, Text, Text, Text> {

    private long currLineNumber;
    private String fileName;
    private long start;
    private Text outKey = new Text();
    private Text outValue = new Text();

    public static String PATTERN = "mapreduce.mapper.regex";
    public static String LINE_NUMBER_DIR ="mapreduce.ngrep.linenumber.dir";
    private Pattern pattern;
    private String lineNumberCountDir;
    private Configuration conf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
        start = split.getStart();
        currLineNumber = 0;

        conf = context.getConfiguration();
        pattern = Pattern.compile(conf.get(PATTERN));
        lineNumberCountDir = conf.get(LINE_NUMBER_DIR);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        assert (name.equals(fileName));

        String text = value.toString();
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            outKey.set(fileName);
            outValue.set(buildValue(text));
            context.write(outKey, outValue);
        }
        currLineNumber++;
    }

    private String buildValue(String value) {
        return start + ":" + currLineNumber + ":" + value;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Path p = new Path(lineNumberCountDir, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
//        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(p.toUri(), conf);
        while (fs.exists(p)) {
            p = new Path(lineNumberCountDir, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        }
        FSDataOutputStream out = fs.create(p);
        out.writeUTF(fileName);
        out.writeLong(start);
        out.writeLong(currLineNumber);
        out.flush();
        out.close();
        super.cleanup(context);
    }
}

class NGrepReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, Long> mapping = new HashMap<String, Long>();
    private Text outKey = new Text();
    private Text outValue = new Text();
    private String lineNumberCountDir;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        lineNumberCountDir = conf.get(NGrepMapper.LINE_NUMBER_DIR);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(lineNumberCountDir));
        Map<String, List<Long[]>> all = new HashMap<String, List<Long[]>>();
        for (FileStatus f: files) {
            FSDataInputStream in = fs.open(f.getPath());
            String fileName = in.readUTF();
            long start = in.readLong();
            long count = in.readLong();
            List<Long[]> l = all.get(fileName);
            if (l == null) {
                l = new ArrayList<Long[]>();
            }
            l.add(new Long[]{start, count});
            all.put(fileName, l);
        }
        for(Map.Entry<String, List<Long[]>> e: all.entrySet()) {
            String fileName = e.getKey();
            List<Long[]> l = e.getValue();
            Collections.sort(l, new Comparator<Long[]>() {
                public int compare(Long[] o1, Long[] o2) {
                    return o1[0].compareTo(o2[0]);
                }
            });
            long curr = 0;
            for (Long[] arr: l) {
                mapping.put(fileName + ":" + arr[0], curr);
                curr += arr[1];
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileName = key.toString();
        for (Text v: values) {
            String line = v.toString();
            int first = line.indexOf(':', 0);
            String start = line.substring(0, first);
            int second = line.indexOf(':', first + 1);
            Long localLineNumber = Long.parseLong(line.substring(first+1, second));
            String text = line.substring(second+1);
            Long lineNumber = mapping.get(fileName + ":" + start) + localLineNumber;
            outKey.set(fileName+":"+lineNumber);
            outValue.set(text);
            context.write(outKey, outValue);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

}