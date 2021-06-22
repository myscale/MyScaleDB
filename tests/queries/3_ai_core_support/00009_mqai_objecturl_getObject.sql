select name from system.functions where name='getObject';
select name from system.data_type_families where name='ObjectToFetch';

create connection conn_00009 provider='AWS' AWS_ROLE_ARN='arn:aws:iam::111111111:role/rolename' AWS_ROLE_EXTERNAL_ID='0000';
select name, arn, external_id from system.connections where name='conn_00009';
drop connection conn_00009;