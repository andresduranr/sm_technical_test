-- create database sm_technicaltest;
 
-- use sm_technicaltest
 
 create table if not exists load_trace(
 trace_id bigint(20) not null auto_increment comment 'Primary key',
 tabl_loaded varchar(50) not null ,
 status int(2) not null ,
 err_message varchar(200) ,
 start_date datetime not null,
 end_date datetime not null,
 rec_number int(20) not null,
 primary key (trace_id)

 );
 
 create table if not exists recipes(
 recipe_id bigint(20) not null , 
 name varchar(50) not null ,
 minutes int(5) default 0 ,
 contributor_id int(10),
 submitted datetime ,
 description varchar(200) ,
 calorie_level int(2),
 n_steps int(4) ,
 n_ingredients int(4) ,
 primary key (recipe_id)
 );
 
 create table if not exists nutrition_recipe(
 nr_id bigint(20) not null auto_increment, 
 recipe_id bigint(20),
 n_cals float(4,2),
 tot_fat_pdv float (4,2),
 sugar_pdv float (4,2),
 sodium_pdv float (4,2),
 protein_pdv float(4,2),
 sat_fat_pdv float (4,2),
 carbs_pdv float (4,2),
 primary key (nr_id),
 constraint foreign key(recipe_id) references recipes(recipe_id) on delete cascade
 );
 
 create table if not exists interaction(
 interaction_id bigint(20) not null auto_increment, 
 user_id bigint(20) not null ,
 recipe_id bigint(20) not null, 
 i_date datetime not null ,
 rating int (2) not null default 0,
 review varchar(10000),
 primary key (interaction_id),
 constraint foreign key(recipe_id) references recipes(recipe_id) on delete cascade
 );
