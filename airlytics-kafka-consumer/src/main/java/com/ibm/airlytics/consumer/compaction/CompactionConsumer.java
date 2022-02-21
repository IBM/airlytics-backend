package com.ibm.airlytics.consumer.compaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.EventFieldsMaps;
import io.prometheus.client.Gauge;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionConsumer  extends AirlyticsConsumer {
    private static final Logger LOGGER = Logger.getLogger(CompactionConsumer.class.getName());

    private static final int hourInMS = 60*60*1000;
    private static final int daysOldToMergeIgnoreSize = 95; //folder older than 95 days have priority in merge

    //Number of waiting tasks
    static final Gauge waitingTasksNumber = Gauge.build()
            .name("airlytics_compaction_waiting_tasks")
            .help("Number of waiting tasks for the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Gauge unmergedDataMB = Gauge.build()
            .name("compaction_unmerged_data_size_MB")
            .help("Size of unmerged data in MB")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "day").register();


    public class ClearEnvironmentOnRebalance implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            queue.clearQueue();

            //terminate any running merge task
            if (runningTask!=null) {
                runningTask.cancel(true);
            }
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            Set<TopicPartition> newPartitions = new HashSet(partitions);

            LOGGER.info("rebalance assigned partitions: " + assignedPartitionsToString(newPartitions));

            if (!equalsPartitionsSet(assignedPartitions, newPartitions)) {
                LOGGER.info("partitions change");
                assignedPartitions=newPartitions;
                rebalancePartitions = true;
            }
            else {
                LOGGER.info("no partition change");
            }
        }
    }

    public class QueueBuilderThread implements Runnable {
        public void run() {
            while (!exit) {
                if (!rebalancePartitions) { //dont sleep if repartition occured
                    try {
                        Thread.sleep(10 * 1000); //10 sec
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                long minutesSinecLastQueueBuild = (System.currentTimeMillis() - lastBuildQueueTime) / 1000 / 60;

                int queueSize =queue.size();

                //build queue only upon repartition/empty queue/build queue interval exceeded
                Calendar now = Calendar.getInstance();
                int hour = now.get(Calendar.HOUR_OF_DAY);

                int buildQueueIntervel = config.getRefreshTasksQueueIntervalMin();
                if (config.getCatchupHour() != -1) {
                    if ( hour>=config.getCatchupHour() && hour<=config.getCatchupHour()+config.getCatchupDurationMin()/60) {
                        buildQueueIntervel = config.getCatchupDurationMin();
                    }
                }

                if (rebalancePartitions || (queueSize == 0) || (minutesSinecLastQueueBuild > buildQueueIntervel)) {
                    rebalancePartitions = false;
                    try {
                        buildQueue();
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOGGER.error("Error building tasks queue: " + e.getMessage());
                    }
                }
            }
        }
    }

    public class TasksRunnerThread implements Runnable {

        public void run() {
            ArrayList<Future<MergeFolderTask.Result>> runningMergeTasks = new ArrayList<Future<MergeFolderTask.Result>>();
            MergeFolderTask waitingMergeTask = null; //next task to run - it wasnt submitted because it is too big
            double currentMergeSizeM = 0.0; //count the size of the tasks currently running

            while (!exit) {
                while (runningTasksCounter.get()<config.getMergeThreads()) {
                    //clean terminated tasks: check their results + update currentMergeSizeM
                    //System.out.println("#########  number of running tasks = " + runningMergeTasks.size());
                    Iterator<Future<MergeFolderTask.Result>> itr = runningMergeTasks.iterator();
                    while (itr.hasNext()) {
                        Future<MergeFolderTask.Result> item = itr.next();
                        if (item.isDone()) {
                            try {
                                MergeFolderTask.Result result = item.get();
                                if (result.error != null) {
                                    //errors that can be ignored and retried later
                                    LOGGER.warn("Cannot merge folder '" + result.path + "':" + result.error);
                                }

                                currentMergeSizeM-= result.maxUnmergedDataSizeM;
                            }
                            catch (Exception e) {
                                LOGGER.error("Error: Exception during folder merge:" + e.getMessage());
                                e.printStackTrace();
                                stop();
                            }
                            itr.remove();
                        }
                    }

                    MergeFolderTask mergeFolderTask = null;
                    if (waitingMergeTask!=null) {
                        //System.out.println("#########  Using waiting task");
                        mergeFolderTask = waitingMergeTask;
                    }
                    else {
                        TasksQueue.Task task = null;
                        //System.out.println("#########  Using new task");
                        synchronized (queue) {
                            waitingTasksNumber.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).set(queue.size());
                            task = queue.getFirstTask();
                        }
                        if (task != null) {
                            try {
                                mergeFolderTask = new MergeFolderTask(task, awsKey, awsSecret, config, tasksCounter, configuredEventsMaps, runningTasksCounter);
                            } catch (IOException e) {
                                LOGGER.warn("Fail calculating max unmerged data size for folder: " + task.getSubFolder() + ": " + e.getMessage());
                                e.printStackTrace();
                                continue;
                            }
                        }
                    }

                    if (mergeFolderTask != null) {
                        updateFieldsMapsUsingNewConfigIfExists();
                        double taskSizeM =  mergeFolderTask.getMaxUnmergedDataSizeM();

                        if (currentMergeSizeM + taskSizeM <= config.getMaxConcurrentMergeSizeM() || runningMergeTasks.size() == 0) {
                            currentMergeSizeM += taskSizeM;
                            LOGGER.info("Start Merge task:" + mergeFolderTask.getQueuedTask().toString());
                            runningTasksCounter.incrementAndGet();
                            Future<MergeFolderTask.Result> future =  executor.submit(mergeFolderTask);
                            runningMergeTasks.add(future);
                            waitingMergeTask = null;
                        }
                        else {
                            waitingMergeTask = mergeFolderTask;
                        }
                    } else {
                        //wait for new tasks
                        try {
                            Thread.sleep(10 * 1000); //10 sec
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                //wait for one of the running tasks to terminate
                try {
                    Thread.sleep(1000); //1 sec
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private CompactionConsumerConfig config;
    private CompactionConsumerConfig newConfig;
    private String awsSecret;
    private String awsKey;
    private TasksQueue queue = new TasksQueue();
    private Future<MergeFolderTask.Result> runningTask = null;
    private Set<TopicPartition> assignedPartitions = new HashSet<TopicPartition>();
    private boolean rebalancePartitions = false;
    private Thread queueBuilder = new Thread (new QueueBuilderThread());
    private Thread tasksRunner = new Thread (new TasksRunnerThread());
    private DataSerializer inputDataSerializer;
    private ThreadPoolExecutor executor;
    private long lastBuildQueueTime;
    private Instant startTime;
    private AtomicLong tasksCounter = new AtomicLong(0);
    //map between a folder and the last time it was merged
    private HashMap<String, Long> lastFolderMergeTimeMap = new HashMap<>();
    private Set<String> unmergedDataLables = new HashSet<>();

    private boolean inputS3Storage;
    private boolean outputS3Storage;
    private String scoreTopicFolder;
    private String usersToDeleteTopicFolder;
    private boolean exit = false;

    private EventFieldsMaps configuredEventsMaps = new EventFieldsMaps();

    private AtomicInteger runningTasksCounter = new AtomicInteger();

    public CompactionConsumer(CompactionConsumerConfig config) throws IOException {
        try {
            Logger.getLogger(Class.forName("org.apache.hadoop.fs.s3a.S3AInputStream")).setLevel(Level.ERROR);
        } catch (ClassNotFoundException e) {
            LOGGER.info("Fail setting hadoop logging level: " + e.getMessage());
        }

        validateConfig(config);
        //create the consumer locally with enable.auto.commit=true and without producer
        this.config = config;

        this.scoreTopicFolder = config.getScoresFolder() + File.separator + config.getTopic() + File.separator;
        this.usersToDeleteTopicFolder = config.getUsersToDeleteFolder() + File.separator + config.getTopic() + File.separator;

        this.inputS3Storage = config.getInputStorageType().equals(STORAGE_TYPE.S3.toString()); //if false => fileSystemStorage
        this.outputS3Storage = config.getOutputStorageType().equals(STORAGE_TYPE.S3.toString()); //if false => fileSystemStorage

        if (inputS3Storage||outputS3Storage) {
            //S3 configuration
            this.awsSecret = Environment.getEnv(AWS_ACCESS_SECRET_PARAM, true);
            this.awsKey = Environment.getEnv(AWS_ACCESS_KEY_PARAM, true);
        }

        if(inputS3Storage) {
            this.inputDataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
        }
        else { //FILE_SYSTEM
            String baseFolder = config.getBaseFolder();
            if (!baseFolder.endsWith(File.separator)) {
                baseFolder = baseFolder+File.separator;
            }
            this.inputDataSerializer = new FSSerializer(baseFolder, config.getIoActionRetries());
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

        if (config.getSecurityProtocol() != null && !config.getSecurityProtocol().equalsIgnoreCase("NONE")) {
            props.put("security.protocol", config.getSecurityProtocol());
        }
        props.put("enable.auto.commit", true);
        props.put("auto.offset.reset","earliest");
        props.put("max.poll.records", config.getMaxPollRecords());
        props.put("max.poll.interval.ms", config.getMaxPollIntervalMs()); //is the interval between polls is greater - the consumer is consider non-responsive. (this is another consumer keep alive)
        props.put("request.timeout.ms", config.getMaxPollIntervalMs()+1000);  //should be greater than max.poll.interval.ms
        props.put("session.timeout.ms", config.getSessionTimeoutMS());
        props.put("fetch.max.bytes", 10*1024); //10K - default is 1M. Since we dont need the polled data but only using it for rebalance - small amount of data is enough

        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getMergeThreads());

        ClearEnvironmentOnRebalance rebalanceCallback = new ClearEnvironmentOnRebalance();

        // Create the consumer using props, and subscribe to the topic and to the rebalance listener
        this.consumer = new KafkaConsumer<>(props);

        LOGGER.info("config = " + config.toString());
        consumer.subscribe(Collections.singletonList(config.getTopic()), rebalanceCallback);

        configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());

        startTime = Instant.now();

        //periodically build queue
        queueBuilder.start();

        //send merge tasks
        tasksRunner.start();

        LOGGER.info("Compaction consumer created with configuration:\n" + config.toString());
    }

    private void validateConfig(CompactionConsumerConfig config) throws IllegalArgumentException{
        if (config.getInputStorageType()==null || config.getInputStorageType().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'inputStorageType' configuration parameter.");
        }
        if (config.getOutputStorageType()==null || config.getOutputStorageType().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'outputStorageType' configuration parameter.");
        }

        if (!config.getInputStorageType().equals(STORAGE_TYPE.FILE_SYSTEM.toString()) && !config.getInputStorageType().equals(STORAGE_TYPE.S3.toString())) {
            throw new IllegalArgumentException("Illegal value for 'inputStorageType' configuration parameter. Can be either S3 or FILE_SYSTEM.");
        }

        if (!config.getOutputStorageType().equals(STORAGE_TYPE.FILE_SYSTEM.toString()) && !config.getOutputStorageType().equals(STORAGE_TYPE.S3.toString())) {
            throw new IllegalArgumentException("Illegal value for 'outputStorageType' configuration parameter. Can be either S3 or FILE_SYSTEM.");
        }

        if (config.getInputStorageType().equals(STORAGE_TYPE.S3.toString()) || config.getOutputStorageType().equals(STORAGE_TYPE.S3.toString())) {
            if(config.getS3Bucket()==null || config.getS3Bucket().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 's3Bucket' configuration parameter.");
            }
            if(config.getS3region()==null || config.getS3region().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 's3region' configuration parameter.");
            }
        }

        if (config.getInputStorageType().equals(STORAGE_TYPE.FILE_SYSTEM.toString()) || config.getOutputStorageType().equals(STORAGE_TYPE.FILE_SYSTEM.toString())) {
            if(config.getBaseFolder()==null || config.getBaseFolder().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 'baseFolder' configuration parameter.");
            }
        }

        if (config.getInputDataFolder()==null || config.getInputDataFolder().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'inputDataFolder' configuration parameter.");
        }

        if (config.getOutputDataFolder()==null || config.getOutputDataFolder().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'outputDataFolder' configuration parameter.");
        }

        if (config.getScoresFolder()==null || config.getScoresFolder().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'scoresFolder' configuration parameter.");
        }

        if (config.getCatchupHour() != -1 && (config.getCatchupHour()<0 ||  config.getCatchupHour()>23)) {
            throw new IllegalArgumentException("Illegal value for 'catchupHour' configuration parameter. Should be -1 or between 0-23");
        }

        if ((config.getCatchupHour() != -1 && config.getCatchupDurationMin()==-1) || (config.getCatchupHour() == -1 && config.getCatchupDurationMin()!=-1)) {
            throw new IllegalArgumentException("Illegal value for 'catchupHour' and 'catchupDurationMin' configuration parameters. Should be both -1 or both not -1");
        }
    }

    private void buildQueue() throws IOException {

        LOGGER.info("******************************** in build queue *********************************");

        synchronized (queue) {
            Instant start = Instant.now();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

            queue.clearQueue();

            //clear unmerged data metrics
            for (String label:unmergedDataLables) {
                unmergedDataMB.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), label).set(0);
            }
            unmergedDataLables.clear();

            addTasksToQueue(inputDataSerializer, formatter);

            lastBuildQueueTime = System.currentTimeMillis();
            LOGGER.info("build queue time = " + Duration.between(start, Instant.now()).toMillis() + " ms. Number of tasks = " + queue.size());
        }
    }

    private void addTasksToQueue(DataSerializer dataSerializer, SimpleDateFormat formatter) throws IOException {
        int smallFoldersCounter = 0;
        int userDeletionFolders = 0;
        int userDeletionFoldersOlderThan7Days = 0;

        //map between shard_day to the associated task
        Map<String, TasksQueue.Task> addedTasksMap = new HashMap<>();

        //go over the assigned partition's folders, find the related shards and build the priority tasks queue
        for (TopicPartition topicPartition : assignedPartitions) {
            int partition = topicPartition.partition();
            for (int i = 0; i <= 9; i++) {
                int shard = partition * 10 + i;
                String shardScoresFolder = scoreTopicFolder + "shard=" + shard + File.separator;

                //add tasks by score file
                LinkedList<String> scoreFiles = dataSerializer.listFilesInFolder(shardScoresFolder, SCORE_FILE_PREFIX, true);
                for (String f : scoreFiles) {
                    int dayPos = f.indexOf("day=");
                    String dayStr = f.substring(dayPos + 4, dayPos + 14);
                    String folder = f.substring(0, f.lastIndexOf(File.separator));

                    String shardDayFolder = getShardDayFolder(shard, dayStr);
                    Long folderLastMergeTime = lastFolderMergeTimeMap.get(shardDayFolder);
                    if (folderLastMergeTime != null && (System.currentTimeMillis() - folderLastMergeTime) < hourInMS) {
                        LOGGER.info("folder " + folder + " was merged less than an hour ago hence skipped.");
                        continue; //dont merge a folder that was merged less than an hour ago
                    }

                    String scoreStr = f.substring(f.indexOf(SCORE_FILE_PREFIX) + SCORE_FILE_PREFIX.length());
                    double originalScore = Double.valueOf(scoreStr);
                    unmergedDataMB.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), dayStr).inc(originalScore);
                    unmergedDataLables.add(dayStr);

                    double weight = originalScore;
                    long daysOld = calcDayDistance(dayStr, formatter);
                    if (originalScore < config.getMinSizeToMergeM() && daysOld < daysOldToMergeIgnoreSize) {
                        smallFoldersCounter++;
                        continue; //dont merge a folder that was merged less than an hour ago
                    }
                    weight += (daysOld * config.getDayWeightFactor()); //increase days distance weight by 3
                    if (daysOld >=daysOldToMergeIgnoreSize) {
                        weight += 1000; //handle tasks older than 90 days first
                    }
                    TasksQueue.Task newTask = new TasksQueue.Task(shard, dayStr, weight, daysOld, originalScore);

                    queue.addTask(newTask);
                    addedTasksMap.put(shard + "_" + dayStr, newTask);
                }

                //add tasks by users to delete files
                String usersToDeleteFolder = usersToDeleteTopicFolder + "shard=" + shard + File.separator;
                HashMap<String, Long> oldestDeleteRequestTimePerFolderMap = new HashMap<>();
                LinkedList<String> usersToDeleteFiles = dataSerializer.listFilesInFolder(usersToDeleteFolder, USER_TO_DELETE_FILE_PREFIX, true);
                for (String f : usersToDeleteFiles) {
                    int filePrefixPos = f.indexOf(USER_TO_DELETE_FILE_PREFIX);
                    String folder = f.substring(0, filePrefixPos);
                    int dayPos = f.indexOf(USER_TO_DELETE_DAY_MARKER);
                    if (dayPos == -1) {
                        LOGGER.error("Illegal users to delete file name:" + f);
                        continue;
                    }
                    String deleteRequestDay = f.substring(dayPos + USER_TO_DELETE_DAY_MARKER.length());
                    long daysOld = calcDayDistance(deleteRequestDay, formatter);
                    if (!oldestDeleteRequestTimePerFolderMap.containsKey(folder) || oldestDeleteRequestTimePerFolderMap.get(folder) < daysOld) {
                        oldestDeleteRequestTimePerFolderMap.put(folder, daysOld);
                    }
                }

                Set<String> usersToDelFolders = oldestDeleteRequestTimePerFolderMap.keySet();
                for (String f : usersToDelFolders) {
                    long oldestRequestAge = oldestDeleteRequestTimePerFolderMap.get(f);

                    int dayPos = f.indexOf("day=");
                    String dayStr = f.substring(dayPos + 4, dayPos + 14);
                    TasksQueue.Task task = addedTasksMap.get(shard + "_" + dayStr);

                    double deletionScore = oldestRequestAge*config.getUsersDeletionAgeFactor();
                    if (oldestRequestAge>=7) {
                        deletionScore += 2000; //user deletion requests that are older than 7 days should be proccessed with high priority.
                        userDeletionFoldersOlderThan7Days++;
                    }

                    if (task == null) {
                        //this folder does not have score files, mean does not have new data to merge
                        queue.addTask(shard, dayStr, deletionScore, oldestRequestAge, 0.0);
                    }
                    else {
                        queue.updateTaskScore(task, task.getScore() + deletionScore);

                    }
                    userDeletionFolders++;
                }
            }
        }
        LOGGER.info("Number of folders smaller than " + config.getMinSizeToMergeM() +"M is :" + smallFoldersCounter);
        LOGGER.info("Number of folders that have user deletion requests: " + userDeletionFolders);
        LOGGER.info("Number of folders that have user deletion requests older than 7 days: " + userDeletionFoldersOlderThan7Days);
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (!isRunning()) {
            return 0;
        }
        
        if (assignedPartitions.size()>0) {
            try {
                Thread.sleep(30 * 1000); //sleep 30 seconds between polls
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        commit();
        return 0;
    }

    @Override
    public void newConfigurationAvailable() {
        CompactionConsumerConfig tmpConfig = new CompactionConsumerConfig();
        try {
            tmpConfig.initWithAirlock();
            newConfig = tmpConfig;
            if (newConfig.getDayWeightFactor()!=this.config.getDayWeightFactor()) {
                LOGGER.info("Update 'dayWeightFactor' configuration from " + this.config.getDayWeightFactor() + " to " + newConfig.getDayWeightFactor());
                this.config.setDayWeightFactor(newConfig.getDayWeightFactor());
            }

            if (newConfig.getRefreshTasksQueueIntervalMin()!=this.config.getRefreshTasksQueueIntervalMin()) {
                LOGGER.info("Update 'refreshTasksQueueIntervalMin' configuration from " + this.config.getRefreshTasksQueueIntervalMin() + " to " + newConfig.getRefreshTasksQueueIntervalMin());
                this.config.setRefreshTasksQueueIntervalMin(newConfig.getRefreshTasksQueueIntervalMin());
            }

            if (newConfig.getMinSizeToMergeM()!=this.config.getMinSizeToMergeM()) {
                LOGGER.info("Update 'minSizeToMergeM' configuration from " + this.config.getMinSizeToMergeM() + " to " + newConfig.getMinSizeToMergeM());
                this.config.setMinSizeToMergeM(newConfig.getMinSizeToMergeM());
            }

            if (newConfig.getUsersDeletionAgeFactor()!=this.config.getUsersDeletionAgeFactor()) {
                LOGGER.info("Update 'usersDeletionAgeFactor' configuration from " + this.config.getUsersDeletionAgeFactor() + " to " + newConfig.getUsersDeletionAgeFactor());
                this.config.setUsersDeletionAgeFactor(newConfig.getUsersDeletionAgeFactor());
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error init Airlock config: " + e.getMessage(), e);
        }
    }

    private static String assignedPartitionsToString(Set<TopicPartition> partitions) {
        StringBuilder sb = new StringBuilder();
        for (TopicPartition p:partitions) {
            sb.append(p.partition());
            sb.append(',');
        }
        return sb.toString();
    }

    public static boolean equalsPartitionsSet (Set<TopicPartition> currentAssignedPartitions, Set<TopicPartition> newAssignedPartitions ){
        if(currentAssignedPartitions == null || newAssignedPartitions ==null){
            return false;
        }

        if(currentAssignedPartitions.size() != newAssignedPartitions.size()){
            return false;
        }

        Set<Integer> set1=new HashSet<Integer>();
        for (TopicPartition p:currentAssignedPartitions) {
            set1.add(p.partition());
        }

        Set<Integer> set2=new HashSet<Integer>();
        for (TopicPartition p:newAssignedPartitions) {
            set2.add(p.partition());
        }

        return set1.containsAll(set2);
    }

    public static long calcDayDistance(String dayStr, SimpleDateFormat formatter) throws IOException {
        try {
            Date day = formatter.parse(dayStr);
            return Duration.between(day.toInstant(), Instant.now()).toDays();
        } catch (ParseException e) {
            e.printStackTrace();
            LOGGER.error("Fail calculating days distance: " + e.getMessage());
            throw new IOException(e);
        }
    }

    public static String getShardDayFolder(int shard, String day) {
        return "shard=" + shard + File.separator + "day=" + day;
    }

    private synchronized void updateFieldsMapsUsingNewConfigIfExists() {
        if (newConfig != null) {
            config.setCommonFieldTypesMap(newConfig.getCommonFieldTypesMap());
            config.setEventsFieldTypesMap(newConfig.getEventsFieldTypesMap());
            newConfig = null;

            //build versioned fields map from the new configuration
            configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());

        }
    }

    public void stop() {
        super.stop();
        LOGGER.info("stopping compaction consumer");
        exit = true;
    }
}
