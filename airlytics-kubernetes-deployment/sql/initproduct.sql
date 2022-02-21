CREATE SCHEMA IF NOT EXISTS <product>;
SET search_path TO <product>;

CREATE TABLE users (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
	id varchar(64) NOT NULL,
	status varchar(32) NOT NULL,
	first_session timestamp NOT NULL,
	last_session timestamp NOT NULL,
	push_token varchar(1024) NULL,
	platform varchar(32) NOT NULL,
	application varchar(32) NOT NULL,
	app_version varchar(32),
	last_push_sent timestamp NULL,
	shard int4 NOT NULL,
	install_date timestamp NULL,
	experiment varchar(128) NULL,
	variant varchar(128) NULL,
	uninstall_date timestamp NULL,
	dev_user bool NULL DEFAULT false,
	premium_product varchar(128) NULL,
	premium_expiration timestamp NULL,
	os_version varchar(32) NULL,
	push_authorization varchar(64) NULL,
	device_model varchar(256) NULL,
	device_country varchar(64) NULL,
	device_language varchar(64) NULL,
	device_timezone int4 NULL, 
	device_platform varchar(64) NULL,
	version_install_date timestamp NULL,
	location_authorization varchar(32) NULL,
	tracking_authorization varchar(64) NULL,
	device_manufacturer varchar(256) NULL,
	raw_device_model varchar(256) NULL,
	carrier varchar(256) NULL,
	experiment_join_date timestamp NULL,
	variant_join_date timestamp NULL,
	premium_auto_renew_status boolean NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
) WITH (fillfactor=80);

CREATE INDEX users_push_token_idx ON users USING btree (push_token);
CREATE INDEX users_shard_idx ON users (shard);
CREATE INDEX users_experiment_variant_idx ON users USING btree (experiment, variant);
CREATE INDEX users_app_version_idx ON users USING btree (app_version);
CREATE INDEX users_install_date_idx ON users USING btree (install_date);
CREATE INDEX users_install_date_day_idx ON users USING btree ( (install_date::date) );
CREATE INDEX users_dev_user_idx ON users (dev_user) WHERE dev_user; 
CREATE INDEX users_last_session_idx ON users (last_session);


CREATE TABLE users_pi (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
	id varchar(64) NOT NULL,
	shard int4 NOT NULL,
	CONSTRAINT users_pi_pkey PRIMARY KEY (id)
) WITH (fillfactor=80);
CREATE INDEX users_pi_shard_idx ON users_pi (shard);

CREATE TABLE purchases (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
	id varchar(1024) NOT NULL,
	product varchar(256) NOT NULL,
	start_date timestamp NULL,
	expiration_date timestamp NULL,
	active bool NOT NULL,
	actual_end_date timestamp NULL,
	auto_renew_status bool NULL,
	auto_renew_status_change_date timestamp NULL,
	period_start_date timestamp NULL,
	periods float8 NULL,
	duration varchar(32) NULL,
	revenue_usd_micros int8 NOT NULL,
	grace bool NULL,
	trial bool NULL,
	is_upgraded bool NULL,
	intro_pricing bool NULL,
	payment_state varchar(64) NULL,
	expiration_reason varchar(32) NULL,
	cancellation_reason varchar(64) NULL,
	survey_response varchar(64) NULL,
	cancellation_date timestamp NULL,
	platform varchar(16) NULL,
	linked_purchase_token varchar(1024) NULL,
	receipt varchar NULL,
	original_transaction_id varchar(32) NULL,
	last_modified_date timestamp NOT NULL,
	modifier_source varchar(128) NOT NULL,
	last_payment_action_date timestamp NULL,
	store_response varchar(32) NULL,
	trial_start_date timestamp NULL,
    trial_end_date timestamp NULL,
	upgraded_from varchar NULL,
	subscription_status varchar(32) NULL,
	CONSTRAINT purchases_pkey PRIMARY KEY (id)
) WITH (fillfactor=80);

CREATE INDEX purchases_expiration_date_idx ON purchases (expiration_date);
CREATE INDEX purchases_receipt_md5_idx ON purchases (md5(receipt));

CREATE TABLE purchases_users (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
 	purchase_id varchar(1024) NOT NULL,
 	user_id varchar(64) NOT NULL,
 	creation_date timestamp NOT NULL,
	shard int4 NOT NULL,
 	CONSTRAINT purchase_user_id_pkey PRIMARY KEY (purchase_id,user_id)
);

CREATE INDEX purchases_users_user_id_idx ON purchases_users USING btree (user_id);
CREATE INDEX purchases_users_shard_idx ON purchases_users USING btree (shard);
CREATE INDEX purchases_users_purchase_id_idx ON purchases_users USING btree (purchase_id);

CREATE VIEW purchases_with_userid AS (
	SELECT b.user_id, b.shard, a.* 
	FROM purchases a LEFT JOIN purchases_users b ON a.id = b.purchase_id
);

CREATE VIEW purchases_with_latest_userid AS (
	SELECT DISTINCT ON (id)
	b.*, a.* 
	FROM purchases a LEFT JOIN purchases_users b ON a.id = b.purchase_id
	ORDER BY id, creation_date DESC
);

CREATE TABLE purchase_events (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
    purchase_id varchar(1024) NOT NULL,
    event_time timestamp NOT NULL,
    name varchar(64) NOT NULL,
    product varchar(256) NOT NULL,
    revenue_usd_micros int8,
    platform varchar(32) NOT NULL,
    expiration_date timestamp NOT NULL,
    expiration_reason varchar(32) NULL,
    auto_renew_status bool NULL,
    payment_state varchar(64),
    upgraded_from varchar,
    upgraded_to varchar,
    cancellation_reason varchar(64) NULL,
    survey_response varchar(64) NULL,
	intro_pricing bool NULL,
	receipt varchar NULL,
    CONSTRAINT purchase_events_id_pkey PRIMARY KEY (purchase_id,name,event_time)
);

CREATE VIEW purchase_events_with_userid AS (
	SELECT pu.user_id,  pu.shard, pu.creation_date, pe.*
	FROM purchase_events pe LEFT JOIN purchases_users pu ON pe.purchase_id = pu.purchase_id
);

CREATE VIEW purchase_events_with_latest_userid AS (
	SELECT DISTINCT ON (pe.purchase_id, pe.event_time, pe.name)
	pu.user_id, pu.shard, pu.creation_date, pe.*
	FROM purchase_events pe LEFT JOIN purchases_users pu ON pe.purchase_id = pu.purchase_id
	ORDER BY pe.purchase_id, pe.event_time, pe.name, pu.creation_date DESC
);

CREATE TABLE poll_answers (
    event_id varchar NOT NULL,
	event_time timestamp NOT NULL,
    shard int NOT NULL,
    user_id varchar NOT NULL,
    poll_id varchar NOT NULL,
    question_id varchar NOT NULL,
    question_title varchar NOT NULL,
    answer_ids _varchar NULL,
    answer_titles _varchar NULL,
    open_answer varchar NULL,
	id serial4 NOT NULL,
    CONSTRAINT poll_answers_pk PRIMARY KEY (event_id)
);
CREATE INDEX poll_answers_user_idx ON poll_answers (user_id);
CREATE INDEX poll_answers_poll_idx ON poll_answers (poll_id);
CREATE INDEX poll_answers_id_idx ON poll_answers (id);
CREATE INDEX poll_answers_event_time_idx ON poll_answers (event_time);

CREATE TABLE poll_answers_pi (
    event_id varchar NOT NULL,
	event_time timestamp NOT NULL,
    shard int NOT NULL,
    user_id varchar NOT NULL,
    poll_id varchar NOT NULL,
    question_id varchar NOT NULL,
    question_title varchar NOT NULL,
    answer_ids _varchar NULL,
    answer_titles _varchar NULL,
    open_answer varchar NULL,
	id serial4 NOT NULL,
    CONSTRAINT poll_answers_pi_pk PRIMARY KEY (event_id)
);

84.228.83.234
CREATE INDEX poll_answers_pi_user_idx ON poll_answers_pi (user_id);
CREATE INDEX poll_answers_pi_poll_idx ON poll_answers_pi (poll_id);
CREATE INDEX poll_answers_pi_id_idx ON poll_answers (id);
CREATE INDEX poll_answers_pi_event_time_idx ON poll_answers_pi (event_time);

CREATE VIEW poll_all_answers AS (
	SELECT *, false as pi FROM poll_answers
	UNION ALL 
	SELECT *, true as pi FROM poll_answers_pi
);

CREATE TABLE poll_answers_cache (
    cache_id varchar NOT NULL,
    content_json varchar NOT NULL,
    CONSTRAINT poll_answers_cache_pk PRIMARY KEY (cache_id)
);

CREATE TABLE user_cohorts (
	/* Uncomment for archive schemas */
	/* archive_date timestamp NULL */
	user_id varchar(64) NOT NULL,
	shard int4 NOT NULL,
	ups_id varchar(64) NULL,
	cohort_id varchar(64) NOT NULL,
	cohort_name varchar(128) NULL,
	cohort_value varchar(128) NULL,
	pending_deletion boolean NOT NULL default false,
	pending_export boolean NOT NULL default true,
	created_at timestamp NOT NULL default NOW()
);
CREATE INDEX user_cohorts_users_idx on user_cohorts(user_id);
CREATE INDEX user_cohorts_cohorts_idx on user_cohorts(cohort_id);
CREATE INDEX user_cohorts_shard_idx on user_cohorts(shard);
CREATE INDEX user_cohorts_cohort_id_created_at_idx ON user_cohorts (cohort_id, created_at DESC);

/* GRANT only on dev applications */
GRANT usage ON SCHEMA <dev_product> to airlytics_dev_ro;
GRANT SELECT ON TABLE users TO airlytics_dev_ro;
/*GRANT SELECT ON TABLE users_archive TO airlytics_dev_ro;*/
GRANT SELECT ON TABLE purchases TO airlytics_dev_ro;
GRANT SELECT ON TABLE purchases_users TO airlytics_dev_ro;
GRANT SELECT ON TABLE purchase_events TO airlytics_dev_ro;
GRANT SELECT ON purchases_with_userid TO airlytics_dev_ro;
GRANT SELECT ON purchases_with_latest_userid TO airlytics_dev_ro;
GRANT SELECT ON purchase_events_with_userid TO airlytics_dev_ro;
GRANT SELECT ON purchase_events_with_latest_userid TO airlytics_dev_ro;
GRANT SELECT ON TABLE poll_answers TO airlytics_dev_ro;
GRANT SELECT ON TABLE poll_answers_cache TO airlytics_dev_ro;

/* GRANT on both dev and production applications */
GRANT usage ON SCHEMA <product> to airlytics_all_ro;
GRANT SELECT ON TABLE users TO airlytics_all_ro;
GRANT SELECT ON TABLE purchases TO airlytics_all_ro;
GRANT SELECT ON TABLE purchases_users TO airlytics_all_ro;
GRANT SELECT ON purchases_with_userid TO airlytics_all_ro;
GRANT SELECT ON purchases_with_latest_userid TO airlytics_all_ro;
GRANT SELECT ON purchase_events_with_userid TO airlytics_all_ro;
GRANT SELECT ON purchase_events_with_latest_userid TO airlytics_all_ro;
GRANT SELECT ON TABLE purchase_events TO airlytics_all_ro;
GRANT SELECT ON TABLE user_cohorts TO airlytics_all_ro;
GRANT SELECT ON TABLE poll_answers TO airlytics_all_ro;
GRANT SELECT ON TABLE poll_answers_cache TO airlytics_all_ro;

/* GRANT on both dev and production applications */
GRANT airlytics_all_ro TO airlytics_all_ro_with_pi;
GRANT SELECT ON TABLE users_pi TO airlytics_all_ro_with_pi;
GRANT SELECT ON TABLE poll_answers_pi TO airlytics_all_ro_with_pi;
GRANT SELECT ON poll_all_answers TO airlytics_all_ro_with_pi;

/* GRANT on both dev and production applications */
GRANT usage ON SCHEMA <product> to airlytics_all_rw;
GRANT SELECT ON TABLE users TO airlytics_all_rw;
GRANT INSERT ON TABLE users TO airlytics_all_rw;
GRANT UPDATE ON TABLE users TO airlytics_all_rw;
GRANT DELETE ON TABLE users TO airlytics_all_rw;
GRANT SELECT ON TABLE users_pi TO airlytics_all_rw;
GRANT INSERT ON TABLE users_pi TO airlytics_all_rw;
GRANT UPDATE ON TABLE users_pi TO airlytics_all_rw;
GRANT DELETE ON TABLE users_pi TO airlytics_all_rw;
GRANT SELECT ON TABLE purchases TO airlytics_all_rw;
GRANT INSERT ON TABLE purchases TO airlytics_all_rw;
GRANT UPDATE ON TABLE purchases TO airlytics_all_rw;
GRANT DELETE ON TABLE purchases TO airlytics_all_rw;
GRANT SELECT ON TABLE purchases_users TO airlytics_all_rw;
GRANT INSERT ON TABLE purchases_users TO airlytics_all_rw;
GRANT UPDATE ON TABLE purchases_users TO airlytics_all_rw;
GRANT DELETE ON TABLE purchases_users TO airlytics_all_rw;
GRANT SELECT ON purchases_with_userid TO airlytics_all_rw;
GRANT SELECT ON purchases_with_latest_userid TO airlytics_all_rw;
GRANT SELECT ON purchase_events_with_userid TO airlytics_all_rw;
GRANT SELECT ON purchase_events_with_latest_userid TO airlytics_all_rw;
GRANT SELECT ON TABLE purchase_events TO airlytics_all_rw;
GRANT INSERT ON TABLE purchase_events TO airlytics_all_rw;
GRANT UPDATE ON TABLE purchase_events TO airlytics_all_rw;
GRANT DELETE ON TABLE purchase_events TO airlytics_all_rw;
GRANT SELECT ON TABLE user_cohorts TO airlytics_all_rw;
GRANT INSERT ON TABLE user_cohorts TO airlytics_all_rw;
GRANT UPDATE ON TABLE user_cohorts TO airlytics_all_rw;
GRANT DELETE ON TABLE user_cohorts TO airlytics_all_rw;
GRANT SELECT ON TABLE poll_answers TO airlytics_all_rw;
GRANT INSERT ON TABLE poll_answers TO airlytics_all_rw;
GRANT UPDATE ON TABLE poll_answers TO airlytics_all_rw;
GRANT DELETE ON TABLE poll_answers TO airlytics_all_rw;
GRANT SELECT ON TABLE poll_answers_pi TO airlytics_all_rw;
GRANT INSERT ON TABLE poll_answers_pi TO airlytics_all_rw;
GRANT UPDATE ON TABLE poll_answers_pi TO airlytics_all_rw;
GRANT DELETE ON TABLE poll_answers_pi TO airlytics_all_rw;
GRANT SELECT ON poll_all_answers TO airlytics_all_rw;
GRANT SELECT ON TABLE poll_answers_cache TO airlytics_all_rw;
GRANT INSERT ON TABLE poll_answers_cache TO airlytics_all_rw;
GRANT UPDATE ON TABLE poll_answers_cache TO airlytics_all_rw;
GRANT DELETE ON TABLE poll_answers_cache TO airlytics_all_rw;
GRANT USAGE ON SEQUENCE poll_answers_id_seq TO airlytics_all_rw;
GRANT USAGE ON SEQUENCE poll_answers_pi_id_seq TO airlytics_all_rw;

GRANT airlytics_all_ro TO airlytics_dashboard;

GRANT airlytics_dev_ro TO airlytics_dashboard_dev;

GRANT airlytics_all_ro_with_pi TO airlytics_dashboard_with_pi;

/* GRANT on both dev and production applications */
GRANT USAGE ON SCHEMA <product> to airlytics_cohorts;
GRANT SELECT ON TABLE users TO airlytics_cohorts;
GRANT SELECT ON TABLE users_pi TO airlytics_cohorts;
GRANT SELECT ON purchases_with_userid TO airlytics_cohorts;
GRANT SELECT ON purchases_with_latest_userid TO airlytics_cohorts;
GRANT SELECT ON TABLE purchase_events TO airlytics_cohorts;
GRANT SELECT ON purchase_events_with_userid TO airlytics_cohorts;
GRANT SELECT ON purchase_events_with_latest_userid TO airlytics_cohorts;
GRANT SELECT ON TABLE poll_answers TO airlytics_cohorts;
GRANT SELECT ON TABLE poll_answers_pi TO airlytics_cohorts;
GRANT SELECT ON TABLE poll_answers_cache TO airlytics_cohorts;
GRANT SELECT ON TABLE user_cohorts TO airlytics_cohorts;
GRANT INSERT ON TABLE user_cohorts TO airlytics_cohorts;
GRANT UPDATE ON TABLE user_cohorts TO airlytics_cohorts;
GRANT DELETE ON TABLE user_cohorts TO airlytics_cohorts;

/* GRANT on both dev and production applications */
GRANT USAGE, CREATE ON SCHEMA <product> to airlytics_data_import;
GRANT SELECT ON TABLE users TO airlytics_data_import;
GRANT SELECT ON TABLE users_pi TO airlytics_data_import;


SET SESSION AUTHORIZATION airlytics_data_import;
ALTER DEFAULT PRIVILEGES FOR USER airlytics_data_import IN SCHEMA <product> GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airlytics_all_rw;
/* No longer needed - added manually in import code based on if the table is PI or not
ALTER DEFAULT PRIVILEGES FOR USER airlytics_data_import IN SCHEMA <product> GRANT SELECT ON TABLES TO airlytics_all_ro; */
ALTER DEFAULT PRIVILEGES FOR USER airlytics_data_import IN SCHEMA <product> GRANT SELECT ON TABLES TO airlytics_cohorts;
