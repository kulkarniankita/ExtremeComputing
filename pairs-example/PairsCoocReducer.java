import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class PairsCoocReducer extends 
Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> 
{
private final static IntWritable SUM = new IntWritable();

@Override
public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
  throws IOException, InterruptedException {
Iterator<IntWritable> iter = values.iterator();
int sum = 0;
while (iter.hasNext()) {
  sum += iter.next().get();
}

SUM.set(sum);
context.write(key, SUM);
}
}