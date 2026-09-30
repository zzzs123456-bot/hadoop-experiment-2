import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Grandparent {
    public static class RelationMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\\s+");
            if (fields.length < 2 ||
                    ("child".equalsIgnoreCase(fields[0]) &&
                     "parent".equalsIgnoreCase(fields[1]))) {
                return;
            }

            String child = fields[0];
            String parent = fields[1];

            outputKey.set(parent);
            outputValue.set("C\t" + child);
            context.write(outputKey, outputValue);

            outputKey.set(child);
            outputValue.set("P\t" + parent);
            context.write(outputKey, outputValue);
        }
    }

    public static class RelationReducer
            extends Reducer<Text, Text, Text, Text> {
        private final Text grandchild = new Text();
        private final Text grandparent = new Text();

        @Override
        protected void reduce(Text middle, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> children = new ArrayList<>();
            List<String> parents = new ArrayList<>();

            for (Text value : values) {
                String[] fields = value.toString().split("\\t", 2);
                if (fields.length == 2 && "C".equals(fields[0])) {
                    children.add(fields[1]);
                } else if (fields.length == 2 && "P".equals(fields[0])) {
                    parents.add(fields[1]);
                }
            }

            for (String child : children) {
                for (String parent : parents) {
                    grandchild.set(child);
                    grandparent.set(parent);
                    context.write(grandchild, grandparent);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法：Grandparent <输入目录> <输出目录>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "grandchild grandparent relation");
        job.setJarByClass(Grandparent.class);
        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
