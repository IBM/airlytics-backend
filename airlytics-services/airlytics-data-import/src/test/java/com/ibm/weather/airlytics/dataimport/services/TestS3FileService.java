package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.ImportJobDefinition;
import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;

public class TestS3FileService extends S3FileService {

    public TestS3FileService(UserFeaturesDao userFeaturesDao) {
        super(userFeaturesDao, "=users");
    }

    @Override
    public ImportJobDefinition buildJobDefinition(DataImportConfig jobRequest, DataImportAirlockConfig config) throws DataImportServiceException {
        ImportJobDefinition job = new ImportJobDefinition(jobRequest, "users", config);
        String header;

        if(jobRequest.getS3File().contains("update")) {

            if (jobRequest.getS3File().endsWith("3.csv")) {
                header = "\"user_id\",\"ai_wears_hats\"";
            } else {
                header = "\"user_id\",\"ai_high_churn_probability\",\"ai_states_current_location_30d\",\"ai_push_message\",\"ai_wears_hats\"";
            }
        } else {

            if (jobRequest.getS3File().endsWith("3.csv")) {
                header = "\"user_id\",\"shard\",\"ai_wears_hats\"";
            } else if (jobRequest.getS3File().endsWith("played.csv")) {
                header = "\"user_id\",\"shard\",\"ai_core_video_played_any_d30\",\"ai_core_video_played_any_d60\",\"ai_core_video_played_any_d7\",\"ai_core_video_played_coronavirus_d30\",\"ai_core_video_played_coronavirus_d60\",\"ai_core_video_played_coronavirus_d7\",\"ai_core_video_played_earthquake_d30\",\"ai_core_video_played_earthquake_d60\",\"ai_core_video_played_earthquake_d7\",\"ai_core_video_played_eco_d30\",\"ai_core_video_played_eco_d60\",\"ai_core_video_played_eco_d7\",\"ai_core_video_played_fire_d30\",\"ai_core_video_played_fire_d60\",\"ai_core_video_played_fire_d7\",\"ai_core_video_played_health_d30\",\"ai_core_video_played_health_d60\",\"ai_core_video_played_health_d7\",\"ai_core_video_played_nature_d30\",\"ai_core_video_played_nature_d60\",\"ai_core_video_played_nature_d7\",\"ai_core_video_played_news_d30\",\"ai_core_video_played_news_d60\",\"ai_core_video_played_news_d7\",\"ai_core_video_played_news_weather_d30\",\"ai_core_video_played_news_weather_d60\",\"ai_core_video_played_news_weather_d7\",\"ai_core_video_played_non_wxviral_d30\",\"ai_core_video_played_non_wxviral_d60\",\"ai_core_video_played_non_wxviral_d7\",\"ai_core_video_played_severe_d30\",\"ai_core_video_played_severe_d60\",\"ai_core_video_played_severe_d7\",\"ai_core_video_played_space_d30\",\"ai_core_video_played_space_d60\",\"ai_core_video_played_space_d7\",\"ai_core_video_played_travel_d30\",\"ai_core_video_played_travel_d60\",\"ai_core_video_played_travel_d7\",\"ai_core_video_played_tropics_d30\",\"ai_core_video_played_tropics_d60\",\"ai_core_video_played_tropics_d7\",\"ai_core_video_played_volcano_d30\",\"ai_core_video_played_volcano_d60\",\"ai_core_video_played_volcano_d7\",\"ai_core_video_played_winter_d30\",\"ai_core_video_played_winter_d60\",\"ai_core_video_played_winter_d7\"";
            } else if (jobRequest.getS3File().endsWith("pi1.csv")) {
                header = "\"user_id\",\"shard\",\"ai_pi_flag\"";
            } else {
                header = "\"user_id\",\"shard\",\"ai_high_churn_probability\",\"ai_states_current_location_30d\",\"ai_push_message\",\"ai_wears_hats\"";
            }

            if(!job.isWithHeader()) {
                // this is an aggregation import
                StringBuilder sb = new StringBuilder();
                sb.append(UserFeaturesDao.USER_ID_COLUMN);

                for(String column : jobRequest.getAffectedColumns()) {
                    sb.append(',').append(column);
                }
                header = sb.toString();
            }
        }
        processCsvHeader(header, job);
        return job;
    }
}
