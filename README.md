legoo: A developer tool for data transfer among `CSV`, `MySQL`, and `Hive` (`HBase` incubating)
=====

`legoo` is a collection of modules to automate data transfer among `CSV`, `MySQL`, and `Hive`. It's written in `Python` and provides ease of programming, flexibility, and extensibility. 

I refer `CSV` as text file with delimiters such as comma, tab, etc. 

There are tools already such as `MySQL LOAD INFILE`, [Sqoop] (http://sqoop.apache.org) etc. then why create a new tool? 

Let me start with `MySQL LOAD INFILE` limitations which can load `CSV` into `MySQL`. First, target table must be pre defined. This would be a challenge if there are a lot of fields in `CSV`. For instance,  we have `CSV` from `salesforce` having 200+ columns. Creating ddl with appropriate column length is frustrating to say the least; second, `CSV` must be local on MySQL server; third, lacking of verification from `CSV` to table count, do not raise error and return non-zero RC when count not match.  

To transfer data between `Hadoop`/`Hive` and `MySQL`, `Sqoop` from [Cloudera] (http://www.cloudera.com) is the best tool available yet. However, the performance did not quite meet our expectations; It crashes if there is carriage return in `MySQL` source, or if there are keyword clashes;  User can not set the `Hive` target dynamically;  It can not create `MySQL` table dynamically when export `Hive` table to `MySQL`; does not support `CSV` to `Hive`; can only connect to `Hive` DB `default`;  on and on ... 

Out of frustration, I built `legoo` during the [trulia](http://www.trulia.com) innovation week! 

* [Prerequisites](#prerequisites)
* [Legoo modules usage](#legoo-modules-usage)
    - [csv_dump](#csv_dump)
    - [csv_to_mysql](#csv_to_mysql)
    - [csv_to_hive](#csv_to_hive)
    - [mysql_to_hive](#mysql_to_hive)
    - [hive_to_mysql](#hive_to_mysql)
    - [mysql_to_csv](#mysql_to_csv)
    - [hive_to_csv](#hive_to_csv)
* [License](#license)



## Prerequisites

Before you can use `legoo`, `Python` module `MySQLdb` and `Hive` must be installed. Update `shebang` and `hive_path` reference to reflect your system configuration. Dir /data/tmp must be created to store tempoarary files.

`legoo` has been tested under `Python 2.6`, `MySQL 5.1`, and `Hive 0.7`. This document assumes you are using a Linux or Linux-like environment. 

## Legoo modules usage
To use `legoo` modules, You specify the module you want to use and the options that control the module. All modules ships with a help module. To display help with all avaialble options and sample usages, enter: `module_name -h` or `module_name --help` I will go over each of those modules briefly in turn. 

### `csv_dump` 
`csv_dump` is a `CSV` viewer with options for `--delimiter` and `--line_number`. It maps each field value to field name defined in header, print them out vertically with line number and column number. `csv_dump` allows user to dig into the middle of file with `--line_number` option. It is extremely handy to investigate data issues in a large `CSV` file having tons of fields. 
##### display help: 
    $ csv_dump -h 
    Usage: csv_dump [options]
    Options:
      -h, --help                                          show this help message and exit
      -d CSV_DELIMITER, --csv_delimiter=CSV_DELIMITE      csv file delimiter, default: [tab]
      -l LINES, --lines=LINES                             number of lines to dump out, default: [2]
      -n LINE_NUMBER, --line_number=LINE_NUMBER           starting line number to dump out, default: [2]
##### example: pretty print for line 505 in tab delimited file: /data/tmp/dim_listing_delta.csv

    $ csv_dump -d 'tab' -n 505 /data/tmp/dim_listing_delta.csv
    
    Line number                         <<<<    [505]
    [c001]  legacy_listing_id           ==>>    [987654321]
    [c002]  legacy_property_id          ==>>    [123456789]
    [c003]  legacy_address_hash         ==>>    [NULL]
    [c004]  agent_id                    ==>>    [NULL]
    [c005]  agent_name                  ==>>    [John Doe]
    [c006]  agent_email                 ==>>    [abc@yahoo.com]
    [c007]  agent_alternate_email       ==>>    [NULL]
    [c008]  agent_phone                 ==>>    [435-123-4567]
    [c010]  feed_id                     ==>>    [3799]
    [c011]  broker_id                   ==>>    [NULL]
    [c012]  broker_name                 ==>>    [D&B Real Estate Cedar City]
    [c013]  status                      ==>>    [For Sale]
    [c014]  mls_id                      ==>>    [41773]
    [c015]  mls_name                    ==>>    [Online Office]
    [c016]  listing_price               ==>>    [1800]
    [c017]  start_date_key              ==>>    [20110826]
    [c018]  end_date_key                ==>>    [99999999]
    [c019]  md5_checksum                ==>>    [e9182549fb921ce935acd2d80a1f7a7d]
    [c020]  hive_date_key               ==>>    [20130530]
    ... [more columns here] ...

## `csv_to_mysql`
`csv_to_mysql` load `CSV` to target `MySQL` table with options `--mysql_create_table`, `--mysql_truncate_table`, `--csv_delimiter`, `--csv_header`, `--csv_optionally_enclosed_by` etc. when `--mysql_create_table` set to 'Y', DDL generated from `CSV` header, column length calculated by scanning the file, and finally run the DDL on target MySQL server to create table. Non zero RC returned and exception raised for errors like `CSV` count not match table count
##### display help: 
    $ csv_to_mysql -h
    Usage: csv_to_mysql [options] sample.csv
    # sample: create table tmp_opp fbased on csv header then load data into table
    [csv_to_mysql --mysql_create_table='y' --mysql_table='tmp_opp' /tmp/opportunity2.csv]
    
    # sample: load data into table tmp_opp
    [csv_to_mysql --mysql_table='tmp_opp' /tmp/opportunity2.csv]
    
    # sample: truncate table tmp_opp first then load data into table
    [csv_to_mysql --mysql_table='tmp_opp' --mysql_truncate_table='Y' /tmp/opportunity2.csv]
    
    # sample: load data into table tmp_opp for csv without header
    [csv_to_mysql --mysql_table='tmp_opp' --mysql_truncate_table='Y' --csv_header='N' /tmp/opportunity3.csv]
    
    
    Options:
      -h, --help                                    show this help message and exit
      --mysql_ini=MYSQL_INI                         mysql initial file for user, password and default db,default: [mysql.ini]
      --mysql_host=MYSQL_HOST                       target mysql host. default: [bidbs]
      --mysql_db=MYSQL_DB                           target mysql database. default: [bi_staging]
      --mysql_table=MYSQL_TABLE                     target mysql table name
      --mysql_create_table=MYSQL_CREATE_TABLE       mysql drop table flag [Y|N]. default: [N]
      --mysql_truncate_table=MYSQL_TRUNCATE_TABLE   mysql truncate table flag [Y|N]. default: [N]
      --csv_delimiter=CSV_DELIMITER                 delimiter for csv file. default: [tab]
      --csv_header=CSV_HEADER                       header flag for csv file. default: [Y]
      --csv_optionally_enclosed_by=CSV_OPTIONALLY_ENCLOSED_BY
                                                    csv_optionally enclosed_by for csv file
      --max_rows=MAX_ROWS                           number of rows in csv file scanned to find column length
      --debug=DEBUG                                 debug flag [Y|N]. default: [N]
##### example: generate `MySQL` `DDL` from `CSV` header, create table based on `DDL`, then load data. note: ommited options take on default value. there are ~250 fields in CSV. Imagine creating `DDL` yourself. 
    
    $ csv_to_mysql --mysql_create_table='y' --mysql_table='tmp_opp' test/opportunity2.csv 
    [INFO] [test/opportunity2.csv] line count ==>> [999] lines
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [CREATE TABLE tmp_opp (
    AccountId                                         VARCHAR(18), 
    Accounting_Approval__c                            VARCHAR(0), 
    Ad_History__c                                     VARCHAR(53), 
    Ad_Server__c                                      VARCHAR(0), 
    Agent_Contract_Length__c                          VARCHAR(6), 
    Agent_Name_formula__c                             VARCHAR(37), 
    Agent_Opportunity_Ranking__c                      VARCHAR(0), 
    AGENT_PHI_Score__c                                VARCHAR(6), 
    Agent_Phone__c                                    VARCHAR(25), 
    AGENT_primary_target_Id_c__c                      VARCHAR(5), 
    Agent_Product_Type__c                             VARCHAR(4), 
    Agent_Referral__c                                 VARCHAR(0), 
    AGENT_target_Id_csv_c__c                          VARCHAR(0), 
    AGENT_Target_Locations__c                         VARCHAR(1641), 
    Agent_ZiP__c                                      VARCHAR(0), 
    Allocation_Type__c                                VARCHAR(11), 
    AM_Notes__c                                       VARCHAR(0), 
    AM_Urgency__c                                     VARCHAR(0), 
    Amount                                            VARCHAR(9), 
    ...  [250 columns here] ... 
    ZSO_Type__c                                       VARCHAR(15) 
    );]
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [LOAD DATA LOCAL INFILE 'test/opportunity2.csv'
      INTO TABLE tmp_opp
      FIELDS TERMINATED BY '\t'    IGNORE 1 LINES]
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [select count(*) from bi_staging.tmp_opp;]
    [INFO] mysql table [bidbs]:[bi_staging].[tmp_opp] row count ==>> [999]
    [INFO] file [test/opportunity2.csv] successfully loaded to mysql table [bidbs]:[bi_staging].[tmp_opp]



## `csv_to_hive` 
`csv_to_hive` load `CSV` file to remote target `Hive` cluster. When option `--hive_create_table` = 'Y', DDL generated from csv header to create hive table. 
##### display help: 
    
    $ csv_to_hive -h
    Usage: csv_to_hive [options]
    
    Options:
      -h, --help                                 show this help message and exit
      --hive_node=HIVE_NODE                      OPTIONAL: target hive node. default: [namenode1]
      --hive_port=HIVE_PORT                      OPTIONAL: target hive port number. default: 10000
      --hive_db=HIVE_DB                          OPTIONAL: target hive db. default: [staging]
      --hive_table=HIVE_TABLE                    OPTIONAL: hive table name. default: created from csv file name
      --hive_partition=HIVE_PARTITION            partition name i.e. date_int=20130428
      --hive_create_table=HIVE_CREATE_TABLE      OPTIONAL: CREATE_TABLE flag for hive table DDL. default: [Y]
      --hive_overwrite=HIVE_OVERWRITE            OPTIONAL: OVERWRITE flag for hive table loading.default: [Y]
      --csv_header=CSV_HEADER                    OPTIONAL: csv_header flag csv file default: [Y]
      --remove_carriage_return=REMOVE_CARRIAGE_RETURN
                                                 OPTIONAL: remove carriage return from mysql source table. default: [N]
      --csv_delimiter=CSV_DELIMITER              delimiter for csv file, default: [tab]
      --debug=DEBUG                              debug flag [Y|N], default: [N]

##### example: generate `Hive` `DDL` from `CSV` header, create table based on `DDL`, then load data to table in `staging` db on `Hive` cluster . note: ommited options take on default value. there are ~250 fields in CSV. Imagine creating `DDL` yourself. 
    
    $ csv_to_hive --hive_create_table='Y' --hive_table='tmp_opp' test/opportunity2.csv 
    [INFO] running hive query on [namenode1]:[staging] ==>> [DROP TABLE IF EXISTS tmp_opportunity2]
    [INFO] running hive query on [namenode1]:[staging] ==>> [CREATE TABLE tmp_opportunity2 (
    AccountId                                     string, 
    Accounting_Approval__c                        string, 
    Ad_History__c                                 string, 
    Ad_Server__c                                  string, 
    Agent_Contract_Length__c                      string, 
    Agent_Name_formula__c                         string, 
    Agent_Opportunity_Ranking__c                  string, 
    AGENT_PHI_Score__c                            string, 
    Agent_Phone__c                                string, 
    AGENT_primary_target_Id_c__c                  string, 
    Agent_Product_Type__c                         string, 
    Agent_Referral__c                             string, 
    AGENT_target_Id_csv_c__c                      string, 
    AGENT_Target_Locations__c                     string, 
    Agent_ZiP__c                                  string, 
    Allocation_Type__c                            string, 
    AM_Notes__c                                   string, 
    AM_Urgency__c                                 string, 
    Amount                                        string, 
    ... [ 250 columns here ] ... 
    ZSO_Type__c                                   string
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
    STORED AS TEXTFILE]
    [INFO] running hdfs clean up ==>> [ssh namenode1 '. .bash_profile; hadoop fs -rm /tmp/tmp_opportunity2 2>/dev/null']
    [INFO] running csv upload to hdfs ==>> [tail -n +2 test/opportunity2.csv | ssh namenode1 'hadoop fs -put - /tmp/tmp_opportunity2']
    [INFO] running hive query on [namenode1]:[staging] ==>> [load data inpath '/tmp/tmp_opportunity2' overwrite into table tmp_opportunity2]
    [INFO] running hive query on [namenode1]:[staging] ==>> [SELECT count(*) from tmp_opportunity2]
    [INFO] [tmp_opportunity2] row count ==>> [999] rows
    [INFO] [test/opportunity2.csv] line count ==>> [999] lines
    [INFO] file [test/opportunity2.csv] successfully loaded to hive table [namenode1]:[staging].[tmp_opportunity2] 
    [INFO] running hive query on [namenode1]:[staging] ==>> [ALTER TABLE tmp_opportunity2 RENAME TO tmp_opp]
    [INFO] running hive query on [namenode1]:[staging] ==>> [DROP TABLE IF EXISTS tmp_opportunity2]
    [INFO] hive table [namenode1]:[staging].[tmp_opp]  successfully built
    
## `mysql_to_hive` 
`mysql_to_hive` transfer data from `MySQL` `table` or `query result` to `Hive` table with options: `--mysql_quick`, `--mysql_table`, `--mysql_query`, `--hive_db`, `--hive_create_table`, `--hive_overwrite`, `--hive_table`, `--hive_partition`, `--remove_carriage_return` etc. When `--hive_create_table` set to 'Y', `Hive` table created based on `MySQL DDL`. For large data transfer with more than 1 million rows, set option `--mysql_quick` to use less resource on `MySQL` server. option `--hive_partition` allows user to transfer data directly to `Hive` partition. `--remove_carriage_return` removes carriage return from the `MySQL` source. For column name key word clashes, column name postfix with _new automatically. Non zero RC returned and exception raised if validation failed such as row count not match from MySQL to Hive. 

## hive_to_mysql 
hive_to_mysql move data from Hive table or query result to MySQL table. When --mysql_create_table set to 'Y', MySQL table created based on Hive table DDL. option --mysql_truncate_table allows use to truncate MySQL table first before loading. Non zero RC returned and exception raised if validation failed such as row count not match from `MySQL` to `Hive`. 

## mysql_to_csv 
## hive_to_csv 
export table/query results from MySQL/Hive to tsv. 


To conclude, legoo is a general purpose tool to transfer data among csv, MySQL and Hive. It can be easily extened with more modules like HBase (work in progress). I had fun building and playing legoo and I hope you do too. 


## License
[Legoo is licensed under the MIT License](https://github.com/trulia/hologram/blob/master/LICENSE.txt)
