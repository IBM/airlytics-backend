CREATE SCHEMA IF NOT EXISTS users;

SET search_path TO users;

GRANT usage ON SCHEMA users to user;

create table if not exists user_cohorts (
	user_id varchar(64) not null,
    shard int4 NOT NULL,
	ups_id varchar(64) null,
	cohort_id varchar(64) not null,
	cohort_name varchar(128) null,
	cohort_value varchar(128) null,
	pending_deletion boolean not null default false,
	pending_export boolean not null default true,
	created_at timestamp not null default NOW()
);

create index user_cohorts_users on user_cohorts(user_id);

create index user_cohorts_shards on user_cohorts(shard);

create index user_cohorts_cohorts on user_cohorts(cohort_id);

create index user_cohorts_created on user_cohorts(created_at);

CREATE TABLE users (
	id varchar(64) NOT NULL,
    shard int4 NOT NULL,
	premium bool NULL,
	application varchar(32) NOT NULL,
	app_version varchar(32),
	premium_expiration timestamp NULL,
	experiment varchar(128) NULL,
	sessions_30d int4 NULL,
	in_app_message_displayed_30d int4 NULL,
	last_session timestamp NOT NULL,
	device_model varchar(256) NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE users_pi (
	id varchar(64) NOT NULL,
	ups_id varchar(64) NULL,
	CONSTRAINT users_pi_pkey PRIMARY KEY (id)
);

CREATE TABLE users.user_features (
	user_id varchar(64) NOT NULL,
	shard int4 NOT NULL,
	test_feature_number int4 NULL,
	test_feature_list varchar(2)[] NULL,
	test_feature_json jsonb NULL,
	created_at timestamp not null default NOW(),
	CONSTRAINT user_features_pkey PRIMARY KEY (user_id)
);

create index users_pi_upsid on users_pi(ups_id);

create index users_application on users(application);
