import java.io.*; 
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import javax.sound.sampled.Line;
import javax.tools.Tool;

import static java.lang.Math.min;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent = new Vector<>();     // the vertex neighbors
    /* ... */

    public short getTag(){
        return tag;
    }

    public void setTag(short tag){
        this.tag=tag;
    }

    public long getGroup(){
        return group;
    }

    public void setGroup(long group){
        this.group=group;
    }

    public long getVID(){
        return VID;
    }

    public void setVID(long VID){
        this.VID=VID;
    }

    public Vector<Long> getAdjacent(){
        return adjacent;
    }

    public void setAdjacent(Vector<Long> adjacent){
        this.adjacent=adjacent;
    }

  public Vertex(){}

    public Vertex(short tag, long group, long VID, Vector<Long> adjacent){
        this.tag=tag;
        this.group=group;
        this.VID=VID;
        this.adjacent=adjacent;
    }

    public Vertex(short tag, long group){
        this.tag=tag;
        this.group=group;
    }

    public final void readFields(DataInput di) throws IOException {
        this.tag = di.readShort();
        this.group = di.readLong();
        this.VID = di.readLong();
        this.adjacent = readVector(di);
    }

    public static Vector readVector(DataInput in) throws IOException {
        Vector<Long> v = new Vector<Long>();
        Long e;
        int i = 1;
        while (i > 0) {
            try {
                if ((e = in.readLong()) != -1) {
                    v.add(e);
                } else {
                    i = 0;
                }
            } catch (EOFException eof) {
                i = 0;
            }

        }
        return v;
    }

    public void write(DataOutput d) throws IOException {
        d.writeShort(this.tag);
        d.writeLong(this.group);
        d.writeLong(this.VID);
        for (Long adjacent1 : this.adjacent) {
            d.writeLong(adjacent1);
        }
    }

    public String toString() {
        return this.tag+" "+this.group+" "+this.VID+" "+this.adjacent;
    }

}

public class Graph{

    //----------------------------------------------------------------------------------------------------------------    Mapper 1

    public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] nodes = line.split(",");
            long VID = Long.parseLong(nodes[0]);

            Vector<Long> adjacent = new Vector<Long>();
            for (int i = 1; i < nodes.length; i++) {
                adjacent.add(Long.parseLong(nodes[i]));
            }

            context.write(new LongWritable(VID), new Vertex((short) 0, VID, VID, adjacent));
        }
    }

    //------------------------------------------------------------------------------------------------------------     Mapper 2

    public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.getVID()), value);

            Long group = value.getGroup();
            Vector<Long> v = new Vector<Long>();

            v = value.getAdjacent();
            for (Long n : v) {
                context.write(new LongWritable(n), new Vertex((short) 1, group));
            }
        }
    }

    public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            Long m = Long.MAX_VALUE;

            Vector<Long> adj=  new Vector<>();

            for (Vertex v : values) {
                if(v.tag==0)
                    adj= v.adjacent;
                m=min(m,v.group);
            }

            context.write(new LongWritable(m),new Vertex((short) 0,m, key.get(),adj));
        }
    }

    //--------------------------------------------------------------------------------------------------------      Mapper 3

    public static class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.getGroup()), new LongWritable(1));
        }
    }

    public static class Reducer3 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long m = 0;
            for (LongWritable v : values) {
                m = m + v.get();
            }
            context.write(key, new LongWritable(m));
        }
    }
    /* ... */

    public static void main ( String[] args ) throws Exception {

        //--------------------------------------------------------------------    Job 1

        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        /* ... First Map-Reduce job to read the graph */

        job1.setJarByClass(Graph.class);

        job1.setMapperClass(Mapper1.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/f0"));

        job1.waitForCompletion(true);

        //-------------------------------------------------------------------------------- Job 2

        for (short i=0; i<5; i++) {
            Job job2 = Job.getInstance();
            /* ... Second Map-Reduce job to propagate the group number */
            job2.setJobName("MyJob2");

            job2.setJarByClass(Graph.class);

            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);

            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.setInputPaths(job2, new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/f"+(i+1)));

            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job3.setJobName("MyJob3");

        job3.setJarByClass(Graph.class);

        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);

        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));

        job3.waitForCompletion(true);
    }
}
