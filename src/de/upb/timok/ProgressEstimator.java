package de.upb.timok;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;

public class ProgressEstimator {
	StopWatch sw = new StopWatch();
	List<Long> durations = new ArrayList<>();
	static AtomicInteger jobsStarted = new AtomicInteger(0);
	static AtomicInteger jobsFinished = new AtomicInteger(0);

	TObjectLongMap<String> jobStartTime = new TObjectLongHashMap<>();

	public ProgressEstimator() {

	}

	public void jobStarted(final String jobQualifier){
		final long currentTime = sw.getTime();
		jobsStarted.incrementAndGet();
		jobStartTime.put(jobQualifier, currentTime);
	}

	public int getJobsFinished() {
		return jobsFinished.get();
	}


	public int getJobCount() {
		return jobsStarted.get();
	}

	public synchronized void start() {
		sw.start();
	}

	public synchronized long jobFinished(final String jobQualifier) {
		jobsFinished.incrementAndGet();
		sw.split();
		final long duration = sw.getSplitTime()-jobStartTime.get(jobQualifier);
		durations.add(duration);
		jobStartTime.remove(jobQualifier);
		return getRemainingTime();
	}

	public synchronized long getLastJobTime() {
		return durations.get(durations.size()-1);
	}

	public synchronized String getLastJobTimeString() {
		return DurationFormatUtils.formatDurationHMS(getLastJobTime());
	}

	public synchronized long getRemainingTime() {
		return average(durations) * (jobsStarted.get() - jobsFinished.get());
	}

	public synchronized String getRemainingTimeString() {
		return DurationFormatUtils.formatDurationHMS(getRemainingTime());
	}

	public synchronized long getAverageJobTime() {
		return average(durations);
	}

	public synchronized String getAverageJobTimeString() {
		return DurationFormatUtils.formatDurationHMS(getAverageJobTime());
	}
	public synchronized long getOverallTime() {
		return sw.getTime();
	}

	public synchronized String getOverallTimeString() {
		return DurationFormatUtils.formatDurationHMS(getOverallTime());
	}

	public static long average(final List<Long> jobDurations2) {
		long sum = 0;
		int size = 0;
		synchronized (jobDurations2) {
			size = jobDurations2.size();
			final Iterator<Long> i = jobDurations2.iterator();
			while (i.hasNext()) {
				sum += i.next();
			}
		}
		if(size == 0){
			return Long.MIN_VALUE;
		}
		return sum / size;
	}
}
