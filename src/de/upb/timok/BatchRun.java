/**
 * This file is part of Batchrunner, a tool for running a lot of java jobs in parallel.
 * Copyright (C) 2013-2015  Timo Klerx
 *
 * Batchrunner is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * Batchrunner is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with Batchrunner.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.upb.timok;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.ptql.ProcessFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class BatchRun {

	Sigar s = new Sigar();
	ProgressEstimator pe;
	@Parameter(names = "-help", help = true, description = "Print the help", hidden = true)
	private final boolean help = false;
	transient private static Logger logger = LoggerFactory.getLogger(BatchRun.class);
	RamGobbler ramGobbler = new RamGobbler();
	@Parameter(names = "-reservedMemory", description = "the memory (in MB) to reserve for the system. If not set, it will be estimated")
	Integer reservedMemory;
	@Parameter(names = "-jarFile", description = "The path to the jar file to execute", required = true)
	Path jarPath;
	@Parameter(names = "-fileType", description = "The config file type (e.g. .xml)", required = true)
	String fileType;
	@Parameter(names = "-maxHeap", description = "The heap size for each job in MB. If not set, will be auto adjusted based on the available RAM and number of cores")
	Integer maxHeap;
	@Parameter(names = "-configFolder", description = "The folder with the config files", required = true)
	Path configFolder;
	@Parameter(names = "-searchRecursive", description = "Set to search config files recursively")
	boolean searchRecursive = false;
	@Parameter(names = "-reserveSystemCore", arity = 1, description = "Set to false s.t. there is no core reserved for the system")
	boolean reserveSystemCore = true;
	@Parameter(names = "-ramMonitorInterval", description = "how long to wait (in ms) until to look for the memory consumption of the running jobs")
	int ramMonitorWaitingTime = 10000;
	@Parameter(names = "-concurrentJobs", description = "how many jobs will run in parallel. If not set this will be the number of cores (maybe -1 for a system core)")
	Integer jobsParameter;
	@Parameter(names = "-forceSettings", description = "Force using the specified settings (can cause paging etc)")
	boolean forceSettings = false;
	@Parameter(names = "-jcommander", description = "Called jar expects config files in jcommander format")
	boolean isJCommanderFormat = false;

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException {
		final BatchRun b = new BatchRun();
		final JCommander commander = new JCommander(b);
		commander.setCaseSensitiveOptions(false);
		commander.setAllowAbbreviatedOptions(true);
		commander.setProgramName("java -jar batchrunner.jar");
		try {
			commander.parse(args);
		} catch (final ParameterException e) {
			commander.usage();
			logger.error("Error while parsing parameters. \n{}", e.getMessage());
			System.exit(1);
		}

		b.run();
	}

	public void run() throws InterruptedException {
		if (help) {
			new JCommander(this).usage();
			System.exit(0);
		}
		int ram = 0;
		if (Files.notExists(configFolder)) {
			logger.error("ConfigFolder {} does not exist. Aborting!", configFolder);
			exitWithUsage();
		}
		final int cores = Runtime.getRuntime().availableProcessors();
		int jobs = cores;
		if (reserveSystemCore) {
			jobs--;
		}
		if (jobs <= 0) {
			logger.warn("Could not create {} concurrent jobs. Setting #jobs=1", jobs);
			jobs = 1;
		}
		try {
			ram = (int) s.getMem().getRam();
			if (reservedMemory == null) {
				final double memPercentageUsed = s.getMem().getUsedPercent();
				reservedMemory = (int) (ram * s.getMem().getUsedPercent() / 100);
				logger.info("Currently {} percent of memory are in use, thus estimating reserved memory to be {} MB", memPercentageUsed, reservedMemory);
			}
			ram -= reservedMemory;
		} catch (final SigarException e1) {
			logger.error("Unexpected SigarException occurred.", e1);
		}
		if (maxHeap == null) {
			if (jobsParameter == null) {
				maxHeapHeuristic(ram, jobs);
			} else if (jobsParameter <= 0) {
				logger.warn("-concurrentJobs was set to {}. This makes no sense", jobsParameter);
				exitWithUsage();
			}
		} else if (maxHeap <= 0) {
			logger.warn("-maxHeap was set to {}. This makes no sense", maxHeap);
			exitWithUsage();
		}
		if (jobsParameter == null) {
			// jobsParameter was not set
			if (maxHeap > 0) {
				if (ram <= 0) {
					jobs = 1;
				}
				jobs = Math.min(jobs, ram / maxHeap);
			} else {
				// maxHeap was not set
				// actually this should never be executed because this case should already be handled before
				maxHeapHeuristic(ram, jobs);
			}
		} else {
			// jobsParameter was set
			jobs = jobsParameter;
			if (jobs > cores) {
				logger.warn("More jobs ({}) than processor cores ({}) are used.", jobs, cores);
			}
			if (maxHeap == null) {
				// maxHeap was not set
				maxHeapHeuristic(ram, jobs);
			} else {
				// maxHeap was set
				logger.warn("-concurrentJobs and -maxHeap was set.");
				final int maxHeapHeuristic = ram / jobs;
				if (maxHeapHeuristic < maxHeap) {
					if (forceSettings) {
						logger.warn("Overriding heuristic because of -forceSettings");
					} else {
						logger.error("The maxHeap heuristic does not suggest to use this configuration. It will result in paging if every job uses its maxHeap. Use -forceSettings if you REALLY want to start with this configuration!");
						exitWithUsage();
					}
				}
			}
		}

		logger.info(
				"Based on the current setting and system resources (reserved memory={} MB; ram={} MB; #cores={}), there will be {} concurrent jobs, each with heapSize={} MB.",
				reservedMemory, ram + reservedMemory, cores, jobs, maxHeap);
		pe = new ProgressEstimator();
		final ExecutorService pool = Executors.newFixedThreadPool(jobs);
		if (gobblePool == null) {
			gobblePool = Executors.newCachedThreadPool();
		}
		ramGobbler.start();
		pe.start();
		try {
			Files.walkFileTree(configFolder, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
					if (!attrs.isDirectory() && file.toString().endsWith(fileType)) {
						pe.jobStarted(file.toAbsolutePath().toString());
						pool.submit(new Runnable() {
							@Override
							public void run() {
								startProcess(jarPath, file);
							}
						});
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
					if (searchRecursive || Files.isSameFile(configFolder, dir)) {
						return super.preVisitDirectory(dir, attrs);
					} else {
						return FileVisitResult.SKIP_SUBTREE;
					}
				}
			});
		} catch (final IOException e) {
			logger.error("Unexpected exception occured.", e);
		}

		pool.shutdown();
		gobblePool.shutdown();
		logger.info("{} jobs submitted. Awaiting pool termination...", pe.getJobCount());
		pool.awaitTermination(100000000, TimeUnit.DAYS);
		ramGobbler.shutdown();

		logger.info("All jobs took: {}", pe.getOverallTimeString());
		logger.info("Average runtime per job: {}", pe.getAverageJobTimeString());
		logger.info("The smallest job needed {} MB RAM.", ramGobbler.minRamNeeded());
		logger.info("The biggest job needed {} MB RAM.", ramGobbler.maxRamNeeded());
		logger.info("The average RAM consumption was: {} MB.", ramGobbler.averageRamNeeded());

	}

	ExecutorService gobblePool = null;

	private void maxHeapHeuristic(final int ram, final int jobs) {
		maxHeap = ram / jobs;
		logger.info("No max heap size was specified, so setting max heap size={} MB ((RAM-ReservedMem)/(#jobs))", maxHeap);
	}

	private void exitWithUsage() {
		new JCommander(this).usage();
		System.exit(1);
	}

	private void startProcess(final Path jarPath, final Path f) {
		final String configString = isJCommanderFormat ? "@" : "" + f.toAbsolutePath().toString();
		// do not use the config folder but maybe the folder where the jar is placed
		final String[] command = new String[] { "java", "-XX:HeapDumpPath=" + jarPath.getParent().toString(), "-XX:+HeapDumpOnOutOfMemoryError",
				"-Xmx" + maxHeap + "M", "-jar", jarPath.toAbsolutePath().toString(), configString };
		logger.info("Starting job with config {}", configString);
		final String jobQualifier = f.toAbsolutePath().toString();
		// System.out.println(Arrays.toString(command));

		final ProcessBuilder pb = new ProcessBuilder(command);
		// pb.redirectErrorStream(true);
		// Process p = Runtime.getRuntime().exec("java -jar sim.jar " +
		// f.getAbsolutePath());
		Process proc;
		int exitVal = -1;
		try {

			proc = pb.start();
			// any error???
			gobbleStream(proc.getErrorStream(), OutputType.ERROR);
			gobbleStream(proc.getInputStream(), OutputType.INFO);

			ramGobbler.jobStarted(jobQualifier);

			exitVal = proc.waitFor();
			logger.info("ExitValue={} for config {}", exitVal, f.toAbsolutePath());
			if (exitVal != 0) {
				logger.warn("WARNING: config {} exited with errorcode={}", f.toAbsolutePath(), exitVal);
			}
			pe.jobFinished(jobQualifier);
			ramGobbler.jobFinished(jobQualifier);
		} catch (IOException | InterruptedException | SigarException e) {
			logger.error("Job for file {} threw an exception", f, e);
		}
		// String commandString = "java -jar " + jarName + " \"" +
		// f.getAbsolutePath() + "\"";
		// System.out.println("commandString: " + commandString);
		// Process proc = Runtime.getRuntime().exec("java -jar " + jarName +
		// " \"" + f.getAbsolutePath() + "\"");

		logger.info("Finished job {} (out of {}). It took {}", pe.getJobsFinished(), pe.getJobCount(), pe.getLastJobTimeString());
		logger.info("Average job duration until now: {}", pe.getAverageJobTimeString());
		logger.info("Estimated remaining time: {}\n", pe.getRemainingTimeString());
	}

	class RamGobbler extends Thread {
		TLongIntMap ramNeededMaxPerJob = new TLongIntHashMap(100, 0.5f, NO_ENTRY_VALUE, NO_ENTRY_VALUE);
		TLongSet runningProcesses = new TLongHashSet();
		// private static final int WAITING_TIME = 60000;
		// private static final int WAITING_TIME = 1000;
		// private static final int WAITING_TIME = 10000;

		private static final int NO_ENTRY_VALUE = -1;

		String query = "State.Name.eq=java,Args.6.eq=";
		TObjectLongMap<String> argPidCache = new TObjectLongHashMap<>();
		HashSet<String> notWatchedProcesses = new HashSet<>();
		Semaphore sem = new Semaphore(1);
		static final long PID_NOT_FOUND = -1;

		AtomicBoolean shutdown = new AtomicBoolean(false);

		public void shutdown() {
			shutdown.set(true);
		}

		// public void jobFinished(long pId) {
		// synchronized (runningProcesses) {
		// runningProcesses.remove(pId);
		// }
		// }
		//
		// public void jobStarted(long pId) {
		// synchronized (runningProcesses) {
		// runningProcesses.add(pId);
		// }
		// }

		public void jobFinished(final String commandArg) throws SigarException, InterruptedException {
			sem.acquire();
			runningProcesses.remove(getPidForCommandArg(commandArg));
			sem.release();
		}

		public void jobStarted(final String commandArg) throws SigarException, InterruptedException {
			sem.acquire();
			// long pid = getPidForCommandArg(commandArg);
			// if (pid != PID_NOT_FOUND) {
			// runningProcesses.add(pid);
			// }
			notWatchedProcesses.add(commandArg);
			sem.release();
		}

		private long getPidForCommandArg(final String commandArg) throws SigarException {
			if (argPidCache.containsKey(commandArg)) {
				return argPidCache.get(commandArg);
			} else {
				final long[] p = ProcessFinder.find(s, query + commandArg);
				logger.debug("Looking for process with commandarg = {}", commandArg);
				if (p.length > 1 || p.length == 0) {
					// something went wrong
					logger.info("Received other than one pid ({}) for job with config {}. Trying again next time!", Arrays.toString(p), commandArg);
					for (int i = 0; i < p.length; i++) {
						logger.debug(Arrays.toString(s.getProcArgs(p[i])));
					}
					// throw new IllegalStateException("Received more than one pid (" + Arrays.toString(p) + ") for job with config " + commandArg);
					notWatchedProcesses.add(commandArg);
					return PID_NOT_FOUND;
				} else {
					logger.debug("Found exactly one process ({}) for commandArg={}", p[0], commandArg);
					argPidCache.put(commandArg, p[0]);
					notWatchedProcesses.remove(commandArg);
					return p[0];
				}
			}
		}

		public int minRamNeeded() {
			if (ramNeededMaxPerJob.size() == 0) {
				return -1;
			}
			final TIntList temp = new TIntArrayList(ramNeededMaxPerJob.valueCollection());
			return temp.min();
		}

		public int maxRamNeeded() {
			if (ramNeededMaxPerJob.size() == 0) {
				return -1;
			}
			final TIntList temp = new TIntArrayList(ramNeededMaxPerJob.valueCollection());
			return temp.max();
		}

		public int averageRamNeeded() {
			if (ramNeededMaxPerJob.size() == 0) {
				return -1;
			}
			final TIntList temp = new TIntArrayList(ramNeededMaxPerJob.valueCollection());
			return temp.sum() / temp.size();
		}

		@Override
		public void run() {
			try {
				Thread.sleep(ramMonitorWaitingTime);
			} catch (final InterruptedException e) {
				logger.error("Unexpected exception occured.", e);
			}
			while (!shutdown.get()) {
				try {
					// logger.debug("Sem.permits={}",sem.availablePermits());
					sem.acquire();
					lookForUnfoundProcesses();
					// logger.debug("runningProcesses.size()={}",runningProcesses.size());
					runningProcesses.forEach(new TLongProcedure() {
						@Override
						public boolean execute(final long pid) {
							try {
								final int currentRamNeeded = (int) s.getProcMem(pid).getResident() / 1000000;
								final int maxRamNeeded = ramNeededMaxPerJob.get(pid);
								if (currentRamNeeded > maxRamNeeded || maxRamNeeded == NO_ENTRY_VALUE) {
									logger.debug("Updating RAM usage for process with pid={} (before={} MB, now={} MB)", pid, maxRamNeeded, currentRamNeeded);
									ramNeededMaxPerJob.put(pid, currentRamNeeded);
								}
							} catch (final SigarException e) {
								logger.error("Unexpected exception occured.", e);
							}
							return true;
						}
					});
					// logger.debug("Finished for each loop.");
					sem.release();
					// logger.debug("Waiting {} ms", WAITING_TIME);
					Thread.sleep(ramMonitorWaitingTime);
				} catch (final InterruptedException e) {
					sem.release();
					logger.error("Unexpected exception occured.", e);
				} finally {
				}
			}
		}

		private void lookForUnfoundProcesses() {
			try {
				if (notWatchedProcesses.size() > 0) {
					@SuppressWarnings("unchecked")
					final Set<String> clonedSet = ((Set<String>) notWatchedProcesses.clone());
					logger.debug("Looking for previously unfound processes...");
					for (final String s : clonedSet) {
						final long pid = getPidForCommandArg(s);
						if (pid != PID_NOT_FOUND) {
							runningProcesses.add(pid);
						}
					}
				}
			} catch (final SigarException e) {
				logger.error("Unexpected exception occured.", e);
			}
		}
	}

	private void gobbleStream(final InputStream is, final OutputType type) {
		gobblePool.execute(() -> {
			try {
				final InputStreamReader isr = new InputStreamReader(is);
				final BufferedReader br = new BufferedReader(isr);
				String line = null;
				while ((line = br.readLine()) != null) {
					if (type == OutputType.INFO) {
						logger.info("{}", line);
					} else if (type == OutputType.ERROR) {
						logger.error("{}", line);
					}
				}
			} catch (final IOException ioe) {
				logger.error("Unexpected IOException occured.", ioe);
			}
		});
	}
}
