package ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


//output value class
public class VolumeWriteable implements Writable {
    private IntWritable count;
    private MapWritable volumeIds;

    private Map<IntWritable,IntWritable> map = new HashMap<>();
    public VolumeWriteable(MapWritable volumeIds, IntWritable count){
        this.count = count;
        this.volumeIds = volumeIds;//#TODO#: initialize class variables
    }

    public VolumeWriteable() {
     count = new IntWritable(0);
     volumeIds = new MapWritable();//#TODO#: initialize class variables with default values
    }

    public IntWritable getCount() {
        return count;
    }

    public MapWritable getVolumeIds() {
        return volumeIds;
    }

    public void set(MapWritable volumeIds, IntWritable count){
        this.volumeIds = volumeIds;
        this.count = count; //#TODO#: set class variables
    }

    public void insertMapValue(IntWritable key, IntWritable value){
        map.put(key,value);

    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        count.readFields(arg0);
        volumeIds.readFields(arg0);
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        count.write(arg0);
        volumeIds.write(arg0);
    }

    @Override
    public String toString(){
        return count.get() + "\t" + volumeIds.size();
    }

    @Override
    public int hashCode() {
        return volumeIds.hashCode();
    }
}
