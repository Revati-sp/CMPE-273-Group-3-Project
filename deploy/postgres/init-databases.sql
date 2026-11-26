-- One logical database per service, so no service can query another's tables.
CREATE DATABASE eh_auth;
CREATE DATABASE eh_tenant;
CREATE DATABASE eh_customer;
CREATE DATABASE eh_session;
CREATE DATABASE eh_human;
CREATE DATABASE eh_product;
CREATE DATABASE eh_order;
CREATE DATABASE eh_notification;
CREATE DATABASE eh_analytics;
CREATE DATABASE eh_audit;
CREATE DATABASE eh_chat;
