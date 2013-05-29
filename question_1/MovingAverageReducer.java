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

/**
 * MovingAverageReducer
 * 
 * Example use of secondary sort and a sliding window to produce a moving
 * average.
 * 
 * As opposed to loading all of the points into a data structure beforehand,
 * this Reducer loads only as many points as are needed to fill the window,
 * continually streaming the points through the window as it receives them.
 * 
 * 
 * Ignores the fact that values may be missing, calculates window based on time
 * delta as opposed to number of samples/points in window.
 * 
 * When only stepping one day we could get away with a simpler algorithm that
 * was more efficient, but this example is meant to show how a full sliding
 * window would work.
 * 
 * Also notice the copying of the points into the sliding window; this is
 * because Hadoop reusues Writables.
 * 
 * 
 * @author jpatterson
 * 
 */
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
	    float point_sum = 0;
	    float moving_avg = 0;


	    long iWindowSizeInMS = 2 * day_in_ms;
	    long iWindowStepSizeInMS = 1 * day_in_ms;

	    SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
		    iWindowStepSizeInMS, day_in_ms);

	    float point_sum_100 = 0;
	    float moving_avg_100 = 0;


	    //long iWindowSizeInMS_100 = iWindowSizeInDays_100 * day_in_ms;
	    //long iWindowStepSizeInMS_100 = iWindowStepSizeInDays_100 * day_in_ms;
	    long iWindowSizeInMS_100 = 3 * day_in_ms;
	    long iWindowStepSizeInMS_100 = 1 * day_in_ms;

	    SlidingWindow sliding_window_100 = new SlidingWindow(iWindowSizeInMS_100,
		    iWindowStepSizeInMS_100, day_in_ms);

	    float point_sum_200 = 0;
	    float moving_avg_200 = 0;


	    long iWindowSizeInMS_200 = 4 * day_in_ms;
	    long iWindowStepSizeInMS_200 = 1 * day_in_ms;

	    SlidingWindow sliding_window_200 = new SlidingWindow(4*day_in_ms,
		    1*day_in_ms, day_in_ms);

	    Text out_key = new Text();
	    Text out_val = new Text();


	    while (values.hasNext()) {
		// Updating the windows, first condition met, update tables if they are not full
		// Update the table until all are full, that means we ignore the stocks for the first 200 days
		// Need to confirm that adding a point into a full window pushes the queue back

		while (sliding_window.WindowIsFull()==false && sliding_window_100.WindowIsFull()==false && sliding_window_200.WindowIsFull()==false && values.hasNext()) {

		    reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

		    next_point = values.next();

		    TimeseriesDataPoint p_copy = new TimeseriesDataPoint();
		    p_copy.copy(next_point);
		    // This is to check if we need to force add into the smaller window, the larger window is automatically checked at the beginning
		    if (sliding_window.WindowIsFull()==false){
			try {
			    sliding_window.AddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }else{
			try {
			    sliding_window.ForceAddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }
		    if (sliding_window_100.WindowIsFull()==false){
			try {
			    sliding_window_100.AddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }else{
			try {
			    sliding_window_100.ForceAddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }
		    // To update the larger window

		    if (sliding_window_200.WindowIsFull()==false){
			try {
			    sliding_window_200.AddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }else{
			try {
			    sliding_window_200.ForceAddPoint(p_copy);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }
		}
		// Only update the key value pairs if all the windows are full
		if (sliding_window.WindowIsFull()&&sliding_window_100.WindowIsFull()&&sliding_window_200.WindowIsFull() ) {

		    reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

		    LinkedList<TimeseriesDataPoint> oWindow = sliding_window
			.GetCurrentWindow();

		    String strBackDate = oWindow.getLast().getDate();
			float actualPrice = oWindow.getLast().fValue;
		    // ---------- compute the moving average here -----------

		    out_key.set( key.getGroup() + ", "
			    + strBackDate + ", " + actualPrice+",");

		    point_sum = 0;

		    for (int x = 0; x < oWindow.size(); x++) {

			point_sum += oWindow.get(x).fValue;

		    } // for

		    moving_avg = point_sum / oWindow.size();

		    LinkedList<TimeseriesDataPoint> oWindow_100 = sliding_window_100
			.GetCurrentWindow();
		    // No need to check the back date, assume that they are all the same
		    //		    String strBackDate = oWindow.getLast().getDate();

		    // ---------- compute the moving average here -----------

		    //		    out_key.set("Group: " + key.getGroup() + ", Date: "
		    //			    + strBackDate);

		    point_sum_100 = 0;

		    for (int x = 0; x < oWindow_100.size(); x++) {

			point_sum_100 += oWindow_100.get(x).fValue;

		    } // for

		    moving_avg_100 = point_sum_100 / oWindow_100.size();

		    LinkedList<TimeseriesDataPoint> oWindow_200 = sliding_window_200
			.GetCurrentWindow();
		    point_sum_200 = 0;

		    for (int x = 0; x < oWindow_200.size(); x++) {

			point_sum_200 += oWindow_200.get(x).fValue;

		    } // for

		    moving_avg_200 = point_sum_200 / oWindow_200.size();

		    out_val.set(moving_avg +", " +  oWindow.size() +  ", " + moving_avg_100 + ", " +  oWindow_100.size() + ", " + moving_avg_200+", " +  oWindow_200.size());
		    //out_val.set(moving_avg + ", " + moving_avg_100 + ", " + moving_avg_200);

		    output.collect(out_key, out_val);

		    // 2. step window forward

		    sliding_window.SlideWindowForward();
		    sliding_window_100.SlideWindowForward();
		    sliding_window_200.SlideWindowForward();

		}

	    } // while

	    output.collect(out_key, out_val);

	} // reduce

}
