package partitioners;

import java.util.Random;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import utils.Protocols;

public class PartitionerRecommendPhase3 extends
		Partitioner<Text, FloatWritable> {

	@Override
	public int getPartition(Text key, FloatWritable value, int numberOfReducers) {
		// TODO Auto-generated method stub
		if (key.toString().startsWith(Protocols.ACCURACY_COUNT) || key.toString().startsWith(Protocols.COLD_START_COUNT)) {
			return 0;
		} else {
			return new Random().nextInt(numberOfReducers-1) + 1;
		}
	}

}
