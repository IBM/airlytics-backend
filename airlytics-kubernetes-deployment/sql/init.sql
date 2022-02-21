
CREATE ROLE airlytics_dev_ro 
NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN PASSWORD '<pass>';

CREATE ROLE airlytics_all_ro 
NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN PASSWORD '<pass2>';

CREATE ROLE airlytics_all_ro_with_pi
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass6>';
GRANT airlytics_all_ro TO airlytics_all_ro_with_pi;

CREATE ROLE airlytics_all_rw 
NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN PASSWORD '<pass3>';
GRANT TEMP on database users to airlytics_all_rw;

CREATE ROLE airlytics_dashboard 
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass4>';
GRANT airlytics_all_ro TO airlytics_dashboard;

CREATE ROLE airlytics_dashboard_dev
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass5>';
GRANT airlytics_dev_ro TO airlytics_dashboard_dev;

CREATE ROLE airlytics_dashboard_with_pi 
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass7>';
GRANT airlytics_all_ro_with_pi TO airlytics_dashboard_with_pi;

CREATE ROLE airlytics_cohorts 
NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN PASSWORD '<pass8>';
GRANT TEMP on database users to airlytics_cohorts;

CREATE ROLE airlytics_data_import 
NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN PASSWORD '<pass9>';
GRANT TEMP on database users to airlytics_data_import;

CREATE ROLE airlytics_onetrust
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass10>';
GRANT airlytics_all_ro_with_pi TO airlytics_onetrust;

CREATE ROLE airlytics_ai_ro 
NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN PASSWORD '<pass11>';
GRANT airlytics_all_ro TO airlytics_ai_ro;

REVOKE ALL PRIVILEGES ON SCHEMA public FROM PUBLIC;

CREATE EXTENSION aws_s3 CASCADE;
GRANT usage ON SCHEMA aws_s3 to airlytics_all_rw;
GRANT usage ON SCHEMA aws_s3 to airlytics_data_import;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA aws_s3 TO airlytics_all_rw;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA aws_s3 TO airlytics_data_import;

REVOKE ALL PRIVILEGES ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO airlytics_all_rw;
GRANT USAGE ON SCHEMA public TO airlytics_data_import;
GRANT USAGE ON SCHEMA public TO airlytics_cohorts;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.awsdms_ddl_audit TO airlytics_all_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.awsdms_ddl_audit TO airlytics_data_import;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.awsdms_ddl_audit TO airlytics_cohorts;
GRANT USAGE, SELECT ON SEQUENCE awsdms_ddl_audit_c_key_seq TO airlytics_all_rw;
GRANT USAGE, SELECT ON SEQUENCE awsdms_ddl_audit_c_key_seq TO airlytics_data_import;
GRANT USAGE, SELECT ON SEQUENCE awsdms_ddl_audit_c_key_seq TO airlytics_cohorts;