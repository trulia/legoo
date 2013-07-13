Legoo: A developer tool for data transfer among `CSV`, `MySQL`, and `Hive` (`HBase` incubating)
=====

`legoo` is a collection of modules to automate data transfer among `CSV`, `MySQL`, and `Hive`. It's written in `Python` and provides ease of programming, flexibility, transparency, and extensibility. 

I refer `CSV` as plain text file with delimiters such as comma, tab, etc. 

There are tools already such as `MySQL LOAD INFILE`, [Sqoop](http://sqoop.apache.org) etc. Then why reinvent the wheel? 

Let me start with `MySQL LOAD INFILE` limitations which load `CSV` into `MySQL`. First, target table must be pre defined. This would be a challenge if there are a lot of fields in `CSV`. For instance,  we have `CSV` from `salesforce` having 200+ columns. Creating ddl with appropriate column length is frustrating to say the least; Second, `CSV` must be local on `MySQL` server; Third, lacking of verification such as `CSV` line count to table count; 

To transfer data between `Hadoop`/`Hive` and `MySQL`, [Sqoop](http://sqoop.apache.org) from [Cloudera](http://www.cloudera.com) is the best tool available. However, the performance did not meet my expectations; It crasheed when there is carriage return or hive keywords (i.e. location) in `MySQL DDL`; User can not set the `Hive` cluster dynamically;  It can not create `MySQL table` dynamically when export `Hive` table to `MySQL`; Nor support `CSV` to `Hive`; only connect to `Hive` DB `default`; the list goes on and on and on and on. 

Out of frustration, I built `legoo` during the [trulia](http://www.trulia.com) innovation week! 

* [Prerequisites](#prerequisites)
* [Legoo modules](#legoo-modules)
    - [csv_dump](#csv_dump)
    - [csv_to_mysql](#csv_to_mysql)
    - [csv_to_hive](#csv_to_hive)
    - [mysql_to_hive](#mysql_to_hive)
    - [hive_to_mysql](#hive_to_mysql)
    - [mysql_to_csv](#mysql_to_csv)
    - [hive_to_csv](#hive_to_csv)
* [Contributors](#contributors)
* [License](#license)


## Prerequisites

Before you can use `legoo`, `Python` module `MySQLdb` and `Hive` must be installed. Update `shebang` and `hive_path` reference to reflect your system configuration. Dir /data/tmp must be created to store tempoarary files.

`legoo` has been tested under `Python 2.6`, `MySQL 5.1`, and `Hive 0.7`. This document assumes you are using a Linux or Linux-like environment. 

## Legoo modules
To use `legoo` modules, You specify the module you want to use and the options that control the module. All modules ships with a help module. To display help with all avaialble options and sample usages, enter: `module_name -h` or `module_name --help` I will go over each of those modules briefly in turn. 

### `csv_dump` 
`csv_dump` is a `CSV` viewer with options for `--delimiter` and `--line_number`. It maps each field value to field name defined in header, print them out vertically with line number and column number. `csv_dump` allows user to dig into the middle of file with `--line_number` option. It is extremely handy to investigate data issues in a large `CSV` file having tons of fields. 
##### `man page`: 
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
##### `man page`: 
    $ csv_to_mysql -h
    Usage: csv_to_mysql [options] sample.csv

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
      --csv_optionally_enclosed_by                  csv_optionally enclosed_by for csv file
      --max_rows=MAX_ROWS                           number of rows in csv file scanned to find column length
      --debug=DEBUG                                 debug flag [Y|N]. default: [N]
##### example: generate `MySQL` `DDL` from `CSV` header, create table based on `DDL`, then load data. note: omitted options take on default value. there are ~250 fields in CSV. Imagine creating `DDL` yourself. 
    
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
`csv_to_hive` load `CSV` into remote `Hive` cluster. When option `--hive_create_table` set to 'Y', `DDL` generated from `CSV` header to create `Hive` table. 
##### `man page`: 
    
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
      --remove_carriage_return                   OPTIONAL: remove carriage return from mysql source table. default: [N]
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
`mysql_to_hive` transfer data from `MySQL` `table` or `query result` to `Hive` table with options: `--mysql_quick`, `--mysql_table`, `--mysql_query`, `--hive_db`, `--hive_create_table`, `--hive_overwrite`, `--hive_table`, `--hive_partition`, `--remove_carriage_return` etc. When `--hive_create_table` set to 'Y', `Hive` table created based on `MySQL DDL`. For large data transfer with millions rows, set option `--mysql_quick` to 'Y' to avoid buffering on `MySQL` server. option `--hive_partition` allows user to transfer data directly to `Hive` partition. `--remove_carriage_return` removes carriage return from the `MySQL` source. For column name key word clashes, column name postfix with _new automatically. Non zero RC returned and exception raised if validation failed such as row count not match from MySQL to Hive. 

##### `man page`:

    $ mysql_to_hive -h
    Usage: mysql_to_hive [options]
    
    Options:
      -h, --help                                        show this help message and exit
      --mysql_ini=MYSQL_INI                             mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host=MYSQL_HOST                           mysql host for source data, default: [bidbs]
      --mysql_db=MYSQL_DB         	       	      	    mysql database for source data, default: [bi]
      --mysql_user=MYSQL_USER                           OPTIONAL: mysql user, if not specified, get user from mysql_ini
      --mysql_password=MYSQL_PASSWORD                   OPTIONAL: mysql password, if not specified, get password from mysql_ini
      --mysql_quick=MYSQL_QUICK                         OPTIONAL: --quick option for mysql client, default:[N]
      --mysql_table=MYSQL_TABLE                         mysql table to be exported
      --mysql_query=MYSQL_QUERY                         query results to be exported
      --hive_node=HIVE_NODE                             OPTIONAL: target hive node. default: [namenode1]
      --hive_port=HIVE_PORT                             OPTIONAL: target hive port. default: 10000
      --hive_db=HIVE_DB                                 OPTIONAL: target hive db. default: [staging]
      --hive_ddl=HIVE_DDL                               OPTIONAL: hive DDL for target hive table. default created from source mysql table
      --hive_create_table=HIVE_CREATE_TABLE             OPTIONAL: CREATE_TABLE flag for hive table DDL. default: [N]
      --hive_overwrite=HIVE_OVERWRITE                   OPTIONAL: OVERWRITE flag for hive table loading. default: [Y]
      --hive_table=HIVE_TABLE                           OPTIONAL: hive table name. default: created from csv file name
      --hive_partition=HIVE_PARTITION                   partition name i.e. date_int=20130428
      --remove_carriage_return=REMOVE_CARRIAGE_RETURN   OPTIONAL: remove carriage return from mysql source table. default: [N]
      --debug=DEBUG                                     set the debug flag [Y|N], default: [N]

##### example: transfer query results from `MySQL` to `Hive` table `email_archive.fe_emailrecord_archive` `partition (date_key=20130710)` on `Hive` cluster. remove carriage return from the source data before transfer. 
    
    $ mysql_to_hive --mysql_host='maildb-slave' --mysql_db='Email' --mysql_table='FE_EmailRecord' --mysql_query="select f.* from Email.FE_EmailRecord f where time_stamp > '2013-05-01' and time_stamp < '2013-05-02'" --hive_table='fe_emailrecord_archive' --hive_db='email_archive'  --remove_carriage_return='Y' --hive_partition="date_key=20130710"
    
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [desc fe_emailrecord_archive]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [DROP TABLE IF EXISTS tmp_FE_EmailRecord]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [CREATE TABLE tmp_FE_EmailRecord (
    id                string, 
    email_type_id     string, 
    user_id_from      string, 
    user_id_to        string, 
    email_from        string, 
    email_to          string, 
    user_message      string, 
    user_copied       string, 
    create_date       string, 
    frequency         string, 
    account_source    string, 
    emailSource       string, 
    guid              string, 
    payload_id        string, 
    time_stamp        string
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
    STORED AS TEXTFILE]
    [INFO] remove special chracter \ with # ==>> [tr -d '\r'  < /data/tmp/FE_EmailRecord.csv > /data/tmp/FE_EmailRecord.csv2]
    [INFO] running hdfs clean up ==>> [ssh namenode1 '. .bash_profile; hadoop fs -rm /tmp/tmp_FE_EmailRecord 2>/dev/null']
    [INFO] running csv upload to hdfs ==>> [tail -n +2 /data/tmp/FE_EmailRecord.csv2 | ssh namenode1 'hadoop fs -put - /tmp/tmp_FE_EmailRecord']
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [load data inpath '/tmp/tmp_FE_EmailRecord' overwrite into table tmp_FE_EmailRecord]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [SELECT count(*) from tmp_FE_EmailRecord]
    [INFO] [tmp_FE_EmailRecord] row count ==>> [3735742] rows
    [INFO] [/data/tmp/FE_EmailRecord.csv2] line count ==>> [3735742] lines
    [INFO] file [/data/tmp/FE_EmailRecord.csv2] successfully loaded to hive table [namenode1]:[email_archive].[tmp_FE_EmailRecord]. 
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [ALTER TABLE fe_emailrecord_archive DROP IF EXISTS PARTITION (date_key=20130710)]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [ALTER TABLE fe_emailrecord_archive ADD PARTITION (date_key=20130710)]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [INSERT OVERWRITE TABLE fe_emailrecord_archive partition (date_key=20130710) select * from tmp_FE_EmailRecord]
    [INFO] running hive query on [namenode1]:[email_archive] ==>> [DROP TABLE IF EXISTS tmp_FE_EmailRecord]
    [INFO] hive table [namenode1]:[email_archive].[fe_emailrecord_archive] PARTITION (date_key=20130710) successfully built
    [INFO] file [/data/tmp/FE_EmailRecord.csv] removed
    [INFO] file [/data/tmp/FE_EmailRecord.csv2] removed



## `hive_to_mysql` 
`hive_to_mysql` transfer data from `Hive table` or `Hive query result` to `MySQL table`. When `--mysql_create_table` set to 'Y', `MySQL table` created based on `Hive table DDL`. option `--mysql_truncate_table` allows user to truncate `MySQL table` first before loading. Non zero RC returned and exception raised if validation failed such as row count not match from `MySQL` to `Hive`. 

##### `man page`

    $ hive_to_mysql -h
    Usage: hive_to_mysql [options]
    
    Options:
      -h, --help                            show this help message and exit
      --hive_node=HIVE_NODE                 source hive node. default: [namenode1]
      --hive_db=HIVE_DB                     source hive database. default: [staging]
      --hive_table=HIVE_TABLE               source hive table name
      --hive_query=HIVE_QUERY               Free form query results to be exported
      --debug=DEBUG                         set the debug flag [Y|N], default: [N]
      --mysql_ini=MYSQL_INI                 mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host=MYSQL_HOST               target mysql host, default: [bidbs]
      --mysql_db=MYSQL_DB                   target mysql database, default: [bi_staging]
      --mysql_table=MYSQL_TABLE             target mysql table name
      --mysql_create_table                  mysql drop table flag [Y|N], default: [N]
      --mysql_truncate_table                mysql truncate table flag [Y|N], default: [N]
      --csv_optionally_enclosed_by          optionally enclosed_by for csv file
      --max_rows=MAX_ROWS                   number of rows scanned to create mysql ddl
      
#####  example: truncate mysql table first then export data from hive table to mysql table 

    $ hive_to_mysql --hive_table='tmp_fpv' --mysql_table='tmp_fpv' --mysql_truncate_table='Y'
    [INFO] running hive export ...
    [ssh namenode1 hive -e "use staging; set hive.cli.print.header=true; select * from staging.tmp_fpv;" > /data/tmp/tmp_fpv.csv]
    
    Hive history file=/tmp/dataproc/hive_job_log_dataproc_201307111627_1487785603.txt
    OK
    Time taken: 1.616 seconds
    OK
    Time taken: 3.42 seconds
    [INFO] hive table namenode1:("use staging; set hive.cli.print.header=true; select * from staging.tmp_fpv;") exported to /data/tmp/tmp_fpv.csv ...
    [INFO] [/data/tmp/tmp_fpv.csv] line count ==>> [689364] lines
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [TRUNCATE TABLE bi_staging.tmp_fpv]
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [LOAD DATA LOCAL INFILE '/data/tmp/tmp_fpv.csv'
      INTO TABLE tmp_fpv
      FIELDS TERMINATED BY '\t'    IGNORE 1 LINES]
    [INFO] running mysql query on [bidbs]:[bi_staging] ==>> [select count(*) from bi_staging.tmp_fpv;]
    [INFO] mysql table [bidbs]:[bi_staging].[tmp_fpv] row count ==>> [689364]
    [INFO] file [/data/tmp/tmp_fpv.csv] successfully loaded to mysql table [bidbs]:[bi_staging].[tmp_fpv]
    [INFO] file [/data/tmp/tmp_fpv.csv] removed

## `mysql_to_csv`
`mysql_to_csv` export `MySQL table` or `MySQL query result` to `TSV`. 

##### `man page`: 

    $ mysql_to_csv -h
    Usage: mysql_to_csv [options] 
    
    Options:
      -h, --help                           show this help message and exit
      --mysql_ini=MYSQL_INI                mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host=MYSQL_HOST              mysql host for source data, default: [bidbs]
      --mysql_db=MYSQL_DB                  mysql database for source data, default: [bi]
      --mysql_user=MYSQL_USER              OPTIONAL: mysql user, if not specified, get user from mysql_ini
      --mysql_password=MYSQL_PASSWORD      OPTIONAL: mysql password, if not specified, get password from mysql_ini
      --mysql_quick=MYSQL_QUICK            mysql quick for large volume data
      --mysql_table=MYSQL_TABLE            mysql table to be exported
      --mysql_query=MYSQL_QUERY            query results to be exported
      --csv_dir=CSV_DIR                    dir for csv file to be exported, default: [/data/tmp]
      --csv_file=CSV_FILE                  the csv file to be exported, default: [table_name.csv]
      --debug=DEBUG                        set the debug flag [Y|N], default: [N]
    
##### example: export `MySQL query result` to `TSV`

    mysql_to_csv --mysql_host='bidbs' --mysql_db='salesforce' --mysql_table='opportunity' --mysql_quick='Y' --mysql_query='select * from opportunity limit 10000' --csv_dir='/data/tmp'
    
##### example: export `MySQL table` to `TSV`
    
    mysql_to_csv --mysql_host='bidbs' --mysql_db='bi' --mysql_table='dim_time' --csv_dir='/data/tmp'


## `hive_to_csv` 
`hive_to_csv` export `Hive table` or `Hive query result` to `TSV`. 

##### `man page`: 

    $ hive_to_csv -h
    Usage: hive_to_csv [options] 
    
    Options:
      -h, --help                      show this help message and exit
      --hive_node=HIVE_NODE           source hive node. default: [namenode1]
      --hive_db=HIVE_DB               source hive database. default: [staging]
      --hive_table=HIVE_TABLE         source hive table name
      --hive_query=HIVE_QUERY         Free form query results to be exported
      --csv_dir=CSV_DIR               dir for tsv
      --csv_file=CSV_FILE             export hive [table | query results] to tsv
      --debug=DEBUG                   set the debug flag [Y|N], default: [N]
    
##### example: export `Hive query result` to `TSV`: `/data/tmp/dim_time.csv`

    hive_to_csv --hive_node='namenode1' --hive_db='bi' --hive_table='dim_time' --hive_query='select * from dim_time limit 1000' --csv_dir='/data/tmp/'

##### example: export `Hive table` to `TSV`: `/data/tmp/dim_time2.csv`

    hive_to_csv --hive_node='namenode1' --hive_db='bi' --hive_table='dim_time' --csv_dir='/tmp/' --csv_file='dim_time2.csv'

##### To conclude, `legoo` is a general purpose tool to transfer data among `CSV`, `MySQL`, `Hive`and `HBase` (incubating). 

## Contributors
* Patrick Luo ([emacsornothing@gmail.com]())

## License
[Legoo is licensed under the MIT License](https://github.com/trulia/legoo/blob/master/LICENSE.txt)
