package com.ibm.airlytics.consumer.dsr.writer;

import com.ibm.airlytics.consumer.dsr.DSRRequest;
import com.ibm.airlytics.consumer.dsr.db.DbHandler;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;


public class DeleteWriter {

    private class ShardData {
        int shard;
        List<JSONObject> userData;
        String minDate;

        public ShardData(int shard, List<JSONObject> userData, String minDate) {
            this.shard=shard;
            this.userData=userData;
            this.minDate=minDate;
        }
    }

    private final DbHandler dbHandler;
    private DeleteWriterConfig config;
    private DeleteNotificationsWriterConfig responseConfig;
    private DataSerializer dataSerializer;
    private DataSerializer devDataSerializer;
    private DataSerializer responseDataSerializer;
    private String dataFilePrefix;
    private String dataTopicFolderDev;
    private String dataTopicFolder;
    private ThreadPoolExecutor executor;
    private boolean dbDumpCleanupEnabled = false;
    private static final AirlyticsLogger logger = AirlyticsLogger.getLogger(DeleteWriter.class.getName());

    public DeleteWriter(DbHandler dbHandler) throws IOException {
        this.dbHandler=dbHandler;
        config = new DeleteWriterConfig();
        config.initWithAirlock();
        String baseFolder = config.getBaseFolder();
        if (!baseFolder.endsWith(File.separator)) {
            baseFolder = baseFolder+File.separator;
        }
        this.dataTopicFolder = config.getTopics().get(0);
        this.dataTopicFolderDev = this.dataTopicFolder+"Dev";
        this.dataFilePrefix = config.getRequestFolder();
        this.dataSerializer = new FSSerializer(baseFolder, config.getWriteRetries());
        String devBaseFolder = config.getDevBaseFolder();
        if (!devBaseFolder.endsWith(File.separator)) {
            devBaseFolder = devBaseFolder+File.separator;
        }
        this.devDataSerializer = new FSSerializer(devBaseFolder, config.getWriteRetries());
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getWriteThreads());

        this.dbDumpCleanupEnabled = config.isDbDumpCleanupEnabled();

        responseConfig = new DeleteNotificationsWriterConfig();
        responseConfig.initWithAirlock();
        this.responseDataSerializer = new S3Serializer(responseConfig.getS3region(), responseConfig.getS3Bucket(), responseConfig.getIoActionRetries());
    }

    public void performDeleteRequestsFromDB(List<DSRRequest> requests, String product) {
        logger.info("perform Delete Requests for product:"+product+" for "+requests.size()+" requests.");
        List<String> upsIds = new ArrayList<>();
        List<String> airlyticsIds = new ArrayList<>();
        List<String> registeredIds = new ArrayList<>();
        for (DSRRequest request : requests) {
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            String upsId = request.getUpsId();
            if (registeredId != null) {
                registeredIds.add(registeredId);
            } else if (deviceId != null) {
                airlyticsIds.add(deviceId);
            } else {
                //if both null the request should be filtered earlier
                upsIds.add(upsId);
            }
        }

        try {
            Map<String, List<String>> registeredUsersMap = null;
            if (registeredIds.size() > 0) {
                registeredUsersMap = dbHandler.getDeviceIdsByRegisteredUser(registeredIds);
                for (List<String> deviceIds : registeredUsersMap.values()) {
                    airlyticsIds.addAll(deviceIds);
                }
            }
            dbHandler.deleteUsersByIds(airlyticsIds, product);
            dbHandler.deleteDevUsersByIds(airlyticsIds, product);
            dbHandler.deleteUsersByUps(upsIds, product);
            dbHandler.deleteDevUsersByUps(upsIds, product);
            //write delete request for compaction
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("error deleting users by dsr request:"+e.getMessage());
            e.printStackTrace();
        }
    }

    class UserData {
        JSONObject userObj;
        DSRRequest dsrRequest;

        public UserData(JSONObject userObj, DSRRequest dsrRequest) {
            this.userObj=userObj;
            this.dsrRequest=dsrRequest;
        }
    }

    private String stripRegisteredId(String airlyticsId) {
        if (airlyticsId == null || config.getRegisteredIdPrefix() == null || config.getRegisteredIdPrefix().isEmpty()) {
            return null;
        }
        if (airlyticsId.startsWith(config.getRegisteredIdPrefix())) {
            return airlyticsId.replaceFirst("^"+config.getRegisteredIdPrefix(),"");
        }
        return null;
    }

    private String stripDeviceId(String airlyticsId) {
        if (airlyticsId == null || config.getDeviceIdPrefix() == null || config.getDeviceIdPrefix().isEmpty()) {
            return airlyticsId;
        }
        if (airlyticsId.startsWith(config.getDeviceIdPrefix())) {
            return airlyticsId.replaceFirst("^"+config.getDeviceIdPrefix(),"");
        }
        return airlyticsId;
    }

    public void writeDeleteNotifications(List<DSRRequest> requests) {
        List<Future<DeleteNotificationWriter.Result>> futures = new ArrayList<>();
        for (DSRRequest request : requests) {
            JSONObject notifObj = getDeleteNotificationJSON(request);
            DeleteNotificationWriter writer = new DeleteNotificationWriter(notifObj, request, responseDataSerializer);
            Future<DeleteNotificationWriter.Result> future = this.executor.submit(writer);
            futures.add(future);
        }
        try {
            for (Future<DeleteNotificationWriter.Result> future : futures) {
                DeleteNotificationWriter.Result result = future.get();
                logger.info("DSR delete notification response for "+result.filePath+": success");
                if (result.error != null) {
                    logger.error("error in delete notification response for "+result.filePath+":"+result.error);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("error writing delete notifications request:"+e.getMessage());
        }
    }

    private JSONObject getDeleteNotificationJSON(DSRRequest request) {
        JSONObject obj = new JSONObject();
        String userId = getUserId(request);
        obj.put("userId", userId);
        obj.put("requestId", request.getRequestId());
        JSONObject statusObj = new JSONObject();
        statusObj.put("requestStatus","success");
        statusObj.put("requestStatusReason","delete request processed");
        obj.put("status",statusObj);
        return obj;
    }

    private String getUserId(DSRRequest request) {
        return request.getAirlyticsId() != null ? request.getAirlyticsId() : request.getUpsId();
    }

    class DeleteNotificationWriter implements Callable {
        JSONObject content;
        DSRRequest request;
        DataSerializer dataSerializer;

        public DeleteNotificationWriter(JSONObject content, DSRRequest request, DataSerializer dataSerializer) {
            this.content = content;
            this.request = request;
            this.dataSerializer = dataSerializer;
        }

        class Result {
            String error = null;
            String filePath;
        }

        @Override
        public Result call() {
            Result res = new Result();
            String id = request.getRequestId();
            String path = composeNotificationFilePath(request.getDayPath(), id);
            res.filePath = path;
            try {
                this.dataSerializer.writeData(path, this.content.toString());
                this.dataSerializer.addFullPermission(path);
            } catch (IOException e) {
                logger.error("error writing delete notification for path: "+path+". "+e.getMessage());
                res.error = e.getMessage();
                e.printStackTrace();
            }
            return res;
        }
    }

    private String composeNotificationFilePath(String requestDayPath, String id) {
        StringBuilder sb = new StringBuilder(responseConfig.getResponseFolder()+File.separator+requestDayPath+File.separator+responseConfig.getResponseSecondPath()+File.separator);
        sb.append(id+".json");
        return sb.toString();
    }

    public void writeDeleteFromParquetRequests(List<DSRRequest> requests, boolean isDev) throws SQLException, ClassNotFoundException {
        if (requests.isEmpty()) {
            return;
        }
        List<String> ids = new ArrayList<>();
        List<String> upsIds = new ArrayList<>();
        List<String> registeredIds = new ArrayList<>();
        for (DSRRequest request : requests) {
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            String upsId = request.getUpsId();
            if (registeredId != null) {
                registeredIds.add(registeredId);
            } else if (deviceId != null) {
                ids.add(deviceId);
            } else {
                //if both null the request should be filtered earlier
                upsIds.add(upsId);
            }
        }
        Map<String, List<String>> registeredUsersMap = null;
        if (registeredIds.size() > 0) {
            registeredUsersMap = dbHandler.getDeviceIdsByRegisteredUser(registeredIds);
            for (List<String> deviceIds : registeredUsersMap.values()) {
                ids.addAll(deviceIds);
            }
        }

        // write delete requests for production users
        Map<String, JSONObject> usersForDeleteByUPS = isDev ? dbHandler.getDevUsersForDelete(upsIds) : dbHandler.getUsersForDelete(upsIds);
        Map<String, JSONObject> usersForDeleteById  = isDev ? dbHandler.getDevUsersForDeleteById(ids) : dbHandler.getUsersForDeleteById(ids);
        logger.info("found "+usersForDeleteByUPS.size()+usersForDeleteById.size()+(isDev ? " dev" : "")+" users to delete out of "+requests.size()+" requests");

        Map<Integer, List<UserData>> usersByShard = groupUsersWithIdByShard(usersForDeleteById, requests);
        usersByShard = addToUsersGroupedByShardWithUps(usersForDeleteByUPS, requests, usersByShard);

        List<Future<WriteShardTask.Result>> futures = new ArrayList<>();
        for (Map.Entry<Integer, List<UserData>> entry : usersByShard.entrySet()) {
            int shard = entry.getKey();
            List<UserData> data = entry.getValue();
            WriteShardTask writeShardTask = new WriteShardTask(shard, data, isDev);
            Future<WriteShardTask.Result> future = this.executor.submit(writeShardTask);
            futures.add(future);
        }

        try {
            for (Future<WriteShardTask.Result> future : futures) {
                WriteShardTask.Result result = future.get();
                logger.info("DSR Response writeResult for "+result.filePath+": success");
                if (result.error != null) {
                    logger.error("error in writeResult for "+result.filePath+":"+result.error);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("error writing production delete requests:"+e.getMessage());
        }


    }

    private Map<Integer, List<UserData>> addToUsersGroupedByShardWithUps(Map<String, JSONObject> usersForDelete, List<DSRRequest> requests, Map<Integer, List<UserData>> toRet) {
        for (Map.Entry<String, JSONObject> entry : usersForDelete.entrySet()) {
            String upsId = entry.getKey();
            JSONObject userObj = entry.getValue();
            int shard = userObj.getInt("shard");
            DSRRequest request = getRequestWithUpsId(requests, upsId);
            UserData data = new UserData(userObj, request);
            List<UserData> usersList = toRet.getOrDefault(shard, new ArrayList<>());
            usersList.add(data);
            toRet.put(shard, usersList);
        }
        return toRet;
    }

    private Map<Integer, List<UserData>> groupUsersWithIdByShard(Map<String, JSONObject> usersForDelete, List<DSRRequest> requests) {
        Map<Integer, List<UserData>> toRet = new HashMap<>();
        for (Map.Entry<String, JSONObject> entry : usersForDelete.entrySet()) {
            String id = entry.getKey();
            JSONObject userObj = entry.getValue();
            int shard = userObj.getInt("shard");
            DSRRequest request = getRequestWithAirlyticsId(requests, id);
            UserData data = new UserData(userObj, request);
            List<UserData> usersList = toRet.getOrDefault(shard, new ArrayList<>());
            usersList.add(data);
            toRet.put(shard, usersList);
        }
        return toRet;
    }

    private DSRRequest getRequestWithUpsId(List<DSRRequest> requests, String upsId) {
        for (DSRRequest request : requests) {
            if (request.getUpsId()!=null && request.getUpsId().equals(upsId)) {
                return request;
            }
        }
        logger.error("did not find dsr-request with ups_id:"+ upsId);
        return null;
    }

    private DSRRequest getRequestWithAirlyticsId(List<DSRRequest> requests, String id) {
        for (DSRRequest request : requests) {
            String airlyticsId = request.getAirlyticsId();
            //string if needed
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            if (registeredId != null) {
                List<String> registered = new ArrayList<>();
                registered.add(registeredId);
                try {
                    Map<String, List<String>> registeredUsersMap = dbHandler.getDeviceIdsByRegisteredUser(registered);
                    for (List<String> deviceIds : registeredUsersMap.values()) {
                        if (deviceIds.size() > 0) {
                            for (String currDevId : deviceIds) {
                                if (currDevId!=null && currDevId.equals(id)) {
                                    return request;
                                }
                            }
                        }
                    }
                } catch (SQLException | ClassNotFoundException e ) {
                    logger.error("error finding device for registered user:"+ e.getMessage());
                    return null;
                }
            } else if (deviceId != null) {
                airlyticsId = deviceId;
            }
            if (airlyticsId!=null && airlyticsId.equals(id)) {
                return request;
            }
        }
        logger.error("did not find dsr-request with airlytics_id:"+ id);
        return null;
    }
    class WriteShardTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }
        private int shard;
        List<UserData> usersData;
        ThreadPoolExecutor executor;
        SimpleDateFormat formatter;
        private boolean isDev;
        
        public WriteShardTask(int shard, List<UserData> usersData, boolean isDev) {
            this.usersData=usersData;
            this.shard=shard;
            this.isDev=isDev;
            this.formatter=new SimpleDateFormat("yyyy-MM-dd");;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getInnerWriteThreads());
        }
        public Result call() {
            Result res = new Result();
            List<Future<WriteUserTask.Result>> futures = new ArrayList<>();
            for (UserData userData : this.usersData) {
                //get firstSession
                Timestamp firstSessionNum = Timestamp.valueOf(userData.userObj.getString("first_session"));
                Date firstSession = new Date(firstSessionNum.getTime());
                List<String> days = getDaysList(firstSession);
                logger.info("will write for shard:"+shard+". days:"+days);
                WriteUserTask userTask = new WriteUserTask(days, userData, shard, isDev);
                Future<WriteUserTask.Result> future = this.executor.submit(userTask);
                futures.add(future);
            }

            for (Future<WriteUserTask.Result> future : futures) {
                    try {
                        WriteUserTask.Result result = future.get();
                        if (result.error != null) {
                            if (res.error == null) {
                                res.error = "";
                            }
                            res.error = res.error + "error in path:"+result.filePath + ":"+result.error+"; ";
                        }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    logger.error("error in WriteUserTask:"+e.getMessage());
                        res.error = res.error + "error in WriteUserTask:"+e.getMessage();
                }

            }
            return res;
        }
        private List<String> getDaysList(Date firstSession) {
            List<String> toRet = new ArrayList<String>();
            toRet.add(formatter.format(firstSession));

            Calendar c = Calendar.getInstance();
            c.setTime(firstSession);

            Date currentDate = new Date(firstSession.getTime());
            Date today = new Date();

            while (currentDate.before(today)) {
                c.add(Calendar.DATE, 1);
                currentDate = c.getTime();
                toRet.add(formatter.format(currentDate));
            }
            return toRet;
        }
    }

    class WriteUserTask implements Callable {
        List<String> days;
        UserData userData;
        int shard;
        boolean isDev;
        ThreadPoolExecutor executor;
        public WriteUserTask(List<String> days, UserData userData, int shard, boolean isDev) {
            this.days = days;
            this.userData = userData;
            this.shard = shard;
            this.isDev = isDev;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getInnerWriteThreads());
        }
        class Result {
            String error = null;
            String filePath;
        }

        public Result call() {
            Result res = new Result();
            String requestDayPath = userData.dsrRequest.getDayPath();
            String userId = userData.userObj.getString("id");

            for (String day : days) {
                WriteEventsMarkerFileTask eventMarkerTask = new WriteEventsMarkerFileTask(shard, day, requestDayPath, userId, this.isDev);
                perform(eventMarkerTask, res);

                if(dbDumpCleanupEnabled) {
                    WriteDbDumpMarkerFileTask dumpMarkerTask = new WriteDbDumpMarkerFileTask(shard, day, requestDayPath, userId, this.isDev);
                    perform(dumpMarkerTask, res);
                }
            }
            return res;
        }

        private void perform(WriteMarkerFileTask task, Result res) {
            WriteEventsMarkerFileTask.Result result = task.call();//no need for a separate thread here

            if (result.error != null) {
                if (res.error == null) {
                    res.error = "";
                }
                res.error = res.error + "error in path:"+result.filePath + ":"+result.error+"; ";
            }
        }

    }

    abstract class WriteMarkerFileTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        int shard;
        String day;
        String requestDayPath;
        String userId;
        boolean isDev = false;

        WriteMarkerFileTask(int shard, String day, String requestDayPath, String userId, boolean isDev) {
            this.shard = shard;
            this.day=day;
            this.requestDayPath=requestDayPath;
            this.userId=userId;
            this.isDev=isDev;
        }

        public Result call() {
            Result res = new Result();
            res.filePath = composeMarkerFilePath(shard, day, requestDayPath, userId, isDev);
            DataSerializer serializer = isDev ? devDataSerializer : dataSerializer;
            logger.info("writing data to:"+res.filePath);
            try {
                serializer.writeData(res.filePath, "");
                logger.info("wrote data to:"+res.filePath);
            } catch (IOException e) {
                logger.error("failed writing data to:"+res.filePath+":"+e.getMessage());
                res.error = e.getMessage();
                e.printStackTrace();
            }

            return res;
        }

        abstract protected String composeMarkerFilePath(int shard, String day, String requestDayPath, String userId, boolean isDev);
    }

    class WriteEventsMarkerFileTask extends WriteMarkerFileTask implements Callable {

        WriteEventsMarkerFileTask(int shard, String day, String requestDayPath, String userId, boolean isDev) {
            super(shard, day, requestDayPath, userId, isDev);
        }

        @Override
        protected String composeMarkerFilePath(int shard, String day, String requestDayPath, String userId, boolean isDev) {
            String format = config.getDeleteFileFormat();
            //strip 'day=' from dayPath
            String path = requestDayPath.replaceFirst("^date=", "");
            //_user_%s_day_%s
            String file = String.format(format, userId, path);
            String topicFolder = isDev ? dataTopicFolderDev : dataTopicFolder;
            return dataFilePrefix+File.separator+topicFolder+File.separator+"shard="+shard+File.separator+"day="+day+File.separator+file;
        }
    }

    class WriteDbDumpMarkerFileTask extends WriteMarkerFileTask implements Callable {

        WriteDbDumpMarkerFileTask(int shard, String day, String requestDayPath, String userId, boolean isDev) {
            super(shard, day, requestDayPath, userId, isDev);
        }

        @Override
        protected String composeMarkerFilePath(int shard, String day, String requestDayPath, String userId, boolean isDev) {
            String format = config.getDeleteFileFormat();
            //strip 'day=' from dayPath
            String path = requestDayPath.replaceFirst("^date=", "");
            //_user_%s_day_%s
            String file = String.format(format, userId, path);
            String topicFolder = isDev ? dataTopicFolderDev : dataTopicFolder;
            return dataFilePrefix+File.separator+"DbDumps"+File.separator+topicFolder+File.separator+"shard="+shard+File.separator+"day="+day+File.separator+file;
        }
    }
}
