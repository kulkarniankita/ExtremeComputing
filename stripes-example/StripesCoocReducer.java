import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StripesCoocReducer extends Reducer<Text, HMapSIW, Text, HMapSIW> {
    @Override
    public void reduce(Text key, Iterable<HMapSIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapSIW> iter = values.iterator();
      HMapSIW map = new HMapSIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }