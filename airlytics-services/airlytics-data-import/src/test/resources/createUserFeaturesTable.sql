CREATE SCHEMA IF NOT EXISTS users;

SET search_path TO users;

GRANT usage ON SCHEMA users to user;

create role airlytics_data_import;
GRANT usage ON SCHEMA users to airlytics_data_import;

create role airlytics_all_rw;
GRANT usage ON SCHEMA users to airlytics_all_rw;

create role airlytics_all_ro_with_pi;
GRANT usage ON SCHEMA users to airlytics_all_ro_with_pi;

create role airlytics_all_ro;
GRANT usage ON SCHEMA users to airlytics_all_ro;

create role airlytics_cohorts;
GRANT usage ON SCHEMA users to airlytics_cohorts;

create table users.user_features (
	user_id varchar(64) not null,
	shard int4 not null,
	ai_high_churn_probability numeric(4, 3) null,
	ai_states_current_location_30d varchar(2)[] null,
	ai_push_message text null,
	ai_wears_hats bool null,
	created_at timestamp not null default NOW(),
	CONSTRAINT user_features_pkey PRIMARY KEY (user_id)
);

create table users.user_features_test (
   user_id varchar(64) not null,
   shard int4 not null,
   ai_high_churn_probability numeric(4, 3) null,
   ai_states_current_location_30d varchar(2)[] null,
   ai_push_message text null,
   ai_wears_hats bool null,
   created_at timestamp not null default NOW(),
   CONSTRAINT user_features_test_pkey PRIMARY KEY (user_id)
);

create table users.user_features_test_pi (
  user_id varchar(64) not null,
  shard int4 not null,
  ai_pi_flag bool null,
  created_at timestamp not null default NOW(),
  CONSTRAINT user_features_test_pi_pkey PRIMARY KEY (user_id)
);

create table users.ad_impressions (
    user_id varchar(64) not null,
    shard int4 not null,
    revenue_d1 int4 null,
    revenue_total int4 null,
    created_at timestamp not null default NOW(),
    CONSTRAINT ad_impressions_pkey PRIMARY KEY (user_id)
);

CREATE TABLE users.users (
	id varchar(64) NOT NULL,
	shard int4 NOT NULL,
	premium bool NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE VIEW users.user_features_all
AS SELECT users.id,
          users.shard,
          a.ai_high_churn_probability,
          a.created_at as test_created_at
   FROM users.users
            LEFT JOIN users.user_features_test a ON users.id = a.user_id;

CREATE OR REPLACE VIEW users.user_features_all_pi
AS SELECT users.id,
          users.shard,
          a.ai_pi_flag,
          a.created_at as test_pi_created_at
   FROM users.users
            LEFT JOIN users.user_features_test_pi a ON users.id = a.user_id;

CREATE TABLE users.user_features_video (
                                           user_id varchar(64) NOT NULL,
                                           shard int4 NOT NULL,
                                           ai_core_video_played_any_d7 int4 NULL,
                                           ai_core_video_played_any_d30 int4 NULL,
                                           ai_core_video_played_any_d60 int4 NULL,
                                           ai_core_video_played_eco_d7 int4 NULL,
                                           ai_core_video_played_eco_d30 int4 NULL,
                                           ai_core_video_played_eco_d60 int4 NULL,
                                           ai_core_video_played_health_d7 int4 NULL,
                                           ai_core_video_played_health_d30 int4 NULL,
                                           ai_core_video_played_health_d60 int4 NULL,
                                           ai_core_video_played_coronavirus_d7 int4 NULL,
                                           ai_core_video_played_coronavirus_d30 int4 NULL,
                                           ai_core_video_played_coronavirus_d60 int4 NULL,
                                           ai_core_video_played_news_weather_d7 int4 NULL,
                                           ai_core_video_played_news_weather_d30 int4 NULL,
                                           ai_core_video_played_news_weather_d60 int4 NULL,
                                           ai_core_video_played_space_d7 int4 NULL,
                                           ai_core_video_played_space_d30 int4 NULL,
                                           ai_core_video_played_space_d60 int4 NULL,
                                           ai_core_video_played_nature_d7 int4 NULL,
                                           ai_core_video_played_nature_d30 int4 NULL,
                                           ai_core_video_played_nature_d60 int4 NULL,
                                           ai_core_video_played_travel_d7 int4 NULL,
                                           ai_core_video_played_travel_d30 int4 NULL,
                                           ai_core_video_played_travel_d60 int4 NULL,
                                           ai_core_video_played_news_d7 int4 NULL,
                                           ai_core_video_played_news_d30 int4 NULL,
                                           ai_core_video_played_news_d60 int4 NULL,
                                           ai_core_video_played_non_wxviral_d7 int4 NULL,
                                           ai_core_video_played_non_wxviral_d30 int4 NULL,
                                           ai_core_video_played_non_wxviral_d60 int4 NULL,
                                           ai_core_video_played_tropics_d7 int4 NULL,
                                           ai_core_video_played_tropics_d30 int4 NULL,
                                           ai_core_video_played_tropics_d60 int4 NULL,
                                           ai_core_video_played_winter_d7 int4 NULL,
                                           ai_core_video_played_winter_d30 int4 NULL,
                                           ai_core_video_played_winter_d60 int4 NULL,
                                           ai_core_video_played_severe_d7 int4 NULL,
                                           ai_core_video_played_severe_d30 int4 NULL,
                                           ai_core_video_played_severe_d60 int4 NULL,
                                           ai_core_video_played_fire_d7 int4 NULL,
                                           ai_core_video_played_fire_d30 int4 NULL,
                                           ai_core_video_played_fire_d60 int4 NULL,
                                           ai_core_video_played_earthquake_d7 int4 NULL,
                                           ai_core_video_played_earthquake_d30 int4 NULL,
                                           ai_core_video_played_earthquake_d60 int4 NULL,
                                           ai_core_video_played_volcano_d7 int4 NULL,
                                           ai_core_video_played_volcano_d30 int4 NULL,
                                           ai_core_video_played_volcano_d60 int4 NULL,
                                           created_at timestamp NOT NULL DEFAULT now(),
                                           CONSTRAINT user_features_video_pkey PRIMARY KEY (user_id)
);

CREATE INDEX users.user_features_video_shards ON users.user_features_video USING btree (shard);

CREATE SCHEMA IF NOT EXISTS users_dev;

SET search_path TO users_dev;

GRANT usage ON SCHEMA users_dev to user;

create table users_dev.ad_impressions (
    user_id varchar(64) not null,
    shard int4 not null,
    revenue_d1 int4 null,
    revenue_total int4 null,
    created_at timestamp not null default NOW(),
    CONSTRAINT ad_impressions_pkey PRIMARY KEY (user_id)
    );

CREATE TABLE users_dev.users (
                       id varchar(64) NOT NULL,
                       shard int4 NOT NULL,
                       premium bool NULL,
                       CONSTRAINT users_pkey PRIMARY KEY (id)
);
