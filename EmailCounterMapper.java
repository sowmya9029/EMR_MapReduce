package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.cmu.lemurproject.WritableWarcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class EmailCounterMapper extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {
		private Text outKey = new Text();
	private static final Logger LOG = Logger.getLogger(EmailCounterMapper.class);
		// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
		// tries to find just the tag name before any spaces, and then consumes any other attributes.
		private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^\\s>]+)([^>]*)>";

	//private static final String EMAIL_PATTERN ="^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})$";
//	    private static final String EMAIL_PATTERN="[A-Za-z0-9+_.-]+@(.+)$";
	private static final String EMAIL_PATTERN="[a-z0-9._%+-]+@[a-z0-9.-]+\\\\.[a-z]{2,6}";
		private Pattern patternTag;
		private Matcher matcherTag;

		@Override
		public void map(LongWritable key, WritableWarcRecord value, Context context) throws IOException {
			try {
				// Compile the regular expression once as it will be used continuously
				patternTag = Pattern.compile(EMAIL_PATTERN, Pattern.CASE_INSENSITIVE);
				//String recordType = value.getRecord().getHeaderRecordType();

				String contentType = value.getRecord().getHeaderString();
				LOG.info("************************************");
				if (contentType.contains("response")) {
					LOG.info("content type " + contentType);
					String body = new String(value.getRecord().getContent());
					Set<Map.Entry<String, String>> urlSet = value.getRecord().getHeaderMetadata().parallelStream()
							.filter(s -> s.getKey().equals("WARC-Target-URI"))
							.distinct()
							.collect(Collectors.toSet());
					String url = urlSet.iterator().next().getValue();
					patternTag = Pattern.compile(EMAIL_PATTERN);
					matcherTag = patternTag.matcher(body);
					while (matcherTag.find()) {
						try {
							context.write(new Text(matcherTag.group()), new Text(url));
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				else
				{
					LOG.info("************not response*********");
				}
			} catch (IllegalStateException e) {

			}
		}
}
