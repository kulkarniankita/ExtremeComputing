import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairsCoocMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    
	private final static PairOfStrings PAIR = new PairOfStrings();
    private final static IntWritable ONE = new IntWritable(1);
    private int window = 2;
    

    @Override
    public void map(LongWritable key, Text line, Context context)
        throws IOException, InterruptedException {
      
      String text = line.toString();

      String[] terms = text.split("\\s+");

      for (int i = 0; i < terms.length; i++) {
        String term = terms[i];

        // skip empty tokens
        if (term.length() == 0)
          continue;

        for (int j = i - window; j < i + window + 1; j++) {
          if (j == i || j < 0)
            continue;

          if (j >= terms.length)
            break;

          // skip empty tokens
          if (terms[j].length() == 0)
            continue;

          // TO-DO 1: call the set() method of PAIR to set the 
          // two elements of the pair (terms i and j)
          
          
          // TO-DO 2: write the key value-pair to the context object,
          // using the write() method.
          // The key is the PAIR object, and the value is ONE.
          
        }
      }
    }
  }