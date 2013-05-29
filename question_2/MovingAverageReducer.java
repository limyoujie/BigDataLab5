package tv.floe.caduceus.hadoop.movingaverage;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovingAverageReducer extends MapReduceBase implements
Reducer<TimeseriesKey, TimeseriesDataPoint, Text, Text> {

    static enum PointCounters {
	POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
    };

    static long day_in_ms = 24 * 60 * 60 * 1000;

    private JobConf configuration;

    @Override
	public void configure(JobConf job) {

	    this.configuration = job;

	}

    public void reduce(TimeseriesKey key, Iterator<TimeseriesDataPoint> values,
	    OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException {

	    TimeseriesDataPoint next_point;
	    float today = 0;
	    float yesterday = 0;
	    int iWindowSizeInDays = this.configuration.getInt(
		    "tv.floe.caduceus.hadoop.movingaverage.windowSize", 2);
	    int iWindowStepSizeInDays = this.configuration.getInt(
		    "tv.floe.caduceus.hadoop.movingaverage.windowStepSize", 1);

	    long iWindowSizeInMS = iWindowSizeInDays * day_in_ms;
	    long iWindowStepSizeInMS = iWindowStepSizeInDays * day_in_ms;

	    Text out_key = new Text();
	    Text out_val = new Text();

	    SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
		    iWindowStepSizeInMS, day_in_ms);

	    while (values.hasNext()) {

		while (sliding_window.WindowIsFull() == false && values.hasNext()) {

		    reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

		    next_point = values.next();

		    TimeseriesDataPoint p_copy = new TimeseriesDataPoint();
		    p_copy.copy(next_point);

		    try {
			sliding_window.AddPoint(p_copy);
		    } catch (Exception e) {
			e.printStackTrace();
		    }

		}

		if (sliding_window.WindowIsFull()) {

		    reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

		    LinkedList<TimeseriesDataPoint> oWindow = sliding_window
			.GetCurrentWindow();

		    String strBackDate = oWindow.getLast().getDate();

		    // ---------- compute the moving average here -----------

		    out_key.set(key.getGroup() + ", "
			    + strBackDate + ", ");
		    today = oWindow.getLast().fValue;
		    yesterday = oWindow.get(0).fValue;


		    out_val.set(today +  ", " + yesterday);

		    output.collect(out_key, out_val);

		    // 2. step window forward

		    sliding_window.SlideWindowForward();

		}

	    } // while


	    output.collect(out_key, out_val);

	} // reduce

}
