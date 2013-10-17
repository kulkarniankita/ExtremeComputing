import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesCoocMapper extends Mapper<LongWritable, Text, Text, HMapSIW> {
    private static final HMapSIW MAP = new HMapSIW();
    private static final Text KEY = new Text();

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

        MAP.clear();

        for (int j = i - window; j < i + window + 1; j++) {
          if (j == i || j < 0)
            continue;

          if (j >= terms.length)
            break;

          // skip empty tokens
          if (terms[j].length() == 0)
            continue;

          MAP.increment(terms[j]);
        }

        KEY.set(term);
        context.write(KEY, MAP);
      }
    }
  }