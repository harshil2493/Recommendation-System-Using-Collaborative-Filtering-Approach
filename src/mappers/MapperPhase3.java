package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Protocols;

public class MapperPhase3 extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String valueRead = value.toString();
		try {
			if (valueRead.startsWith(Protocols.TRAINING_DATASET_STARTS_WITH)) {
				valueRead = valueRead
						.substring(Protocols.TRAINING_DATASET_STARTS_WITH
								.length());
				String[] followedFriends = valueRead
						.split(Protocols.FRIEND_EDGE_SPLIT);

				if (followedFriends.length == 2) {
					String[] listOfFriends = followedFriends[1]
							.split(Protocols.FRIEND_LIST_SEPARATOR);
					if (listOfFriends.length != 0) {
						for (String eachFollowedFriends : listOfFriends) {
							if (!eachFollowedFriends.isEmpty()) {
								context.write(
										new IntWritable(Integer
												.parseInt(eachFollowedFriends)),
										new IntWritable(Integer
												.parseInt(followedFriends[0])));
							}
						}
					}
				}
			}
		} catch (Exception exception) {
			System.out.println("Number Format Exception For String: "
					+ valueRead);
		}
	}
}
