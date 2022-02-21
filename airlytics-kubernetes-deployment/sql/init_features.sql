SET search_path TO <product>;
SET SESSION AUTHORIZATION airlytics_data_import;

CREATE TABLE user_features_sample (
	user_id varchar(64) NOT NULL,
	shard int4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT now(),
	CONSTRAINT user_features_retention_pkey PRIMARY KEY (user_id)
);
CREATE INDEX user_features_sample_shard ON user_features_sample USING btree (shard);

