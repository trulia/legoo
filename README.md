Legoo: A collection of automation modules to build analytics infrastructure
=====

`legoo` is a collection of automation modules to build analytics infrastructure consists `CSV`, `MySQL`, `Hive` and `HBase` (incubating). It's written in `Python` and provides ease of programming, flexibility, transparency, and extensibility.

I refer `CSV` as plain text file with delimiters such as comma, tab, etc.

There are tools already such as `MySQL LOAD INFILE`, [Apache Sqoop](http://sqoop.apache.org) etc. Then why reinvent the wheel?

Let me start with `MySQL LOAD INFILE` limitations that load `CSV` into `MySQL`. First, target table must be pre defined. This would be a challenge if there were a lot of fields in `CSV`. For instance, we have `CSV` from `salesforce` having 200+ columns. Creating `MySQL DDL` with appropriate column length is frustrating to say the least; second, user need to memorize the `load infile` syntax with variious opitons, i.e. `local`, `ignore lines`, `optionally enclosed by` etc; Third, lacking of verification on `CSV` line count to table row count;

To transfer data between `Hive` and `MySQL`, [Apache Sqoop](http://sqoop.apache.org) from [Cloudera](http://www.cloudera.com) is the best tool available. However, as of 4/1/2013, the performance did not meet my expectations; It crashed when there is carriage return or hive keywords (i.e. location) in `MySQL` source data; User can not set the `Hive` cluster dynamically; It can not create `MySQL table` dynamically when export `Hive` table to `MySQL`; Nor support `CSV` to `Hive`; only connect to `Hive` DB `default`; can not specify mapred job priority; the list goes on and on and on and on.

Out of frustration and desperation, `legoo` created! Overtime, more modules added, such as `MySQL` and `Hive` client, dependency handling, QA, notification etc. 

for ease of programming, I created modules, which are wrapper scripts with python function call. 

Here is the high level view of ETL architecture and modules. More details covered in [Legoo modules](#legoo-modules).

![diagram](https://raw.github.com/trulia/legoo/master/ppt/diagram_arch.jpg "architecture diagram")
![diagram](https://raw.github.com/trulia/legoo/master/ppt/diagram_module.jpg "module diagram")

* [Prerequisites](#prerequisites)
* [Unit Test](#unit-test)
* [Legoo Modules](#legoo-modules)
    - [csv_dump](#csv_dump)
    - [csv_to_mysql](#csv_to_mysql)
    - [csv_to_hive](#csv_to_hive)
    - [mysql_to_csv](#mysql_to_csv)
    - [mysql_to_hive](#mysql_to_hive)
    - [hive_to_csv](#hive_to_csv)
    - [hive_to_mysql](#hive_to_mysql)
    - [execute_mysql_query](#execute_mysql_query)
    - [execute_hive_query](#execute_hive_query)
    - [wait_for_file](#wait_for_file)
    - [wait_for_table](#wait_for_table)
    - [qa_mysql_table](#qa_mysql_table)
    - [send_mail](#send_mail)
* [Future Release](#future-release)
* [Contributors](#contributors)
* [License](#license)


## Prerequisites

* Install `Python` module `MySQLdb`.
* Create /data/tmp to store temporary files.
* Hive configuration
    -  find path for `Hive Python` client and add to `sys.path` if different than default location: `/usr/lib/hive/lib/py`
    -  To avoid `hdfs` file permission conflict especially for `Hive` partition tables, start thrift server `HiveServer` on `Hive` cluster using the same user as legoo user. 
    -  Set up `SSH` login without password from server `legoo` modules being run to `Hive` cluster. Typical setup: run `legoo` modules from `command center` server which reference remote `MySQL` servers and remote `Hive` cluster. 

`modules` have been tested on `Python 2.6`, `Python 2.7`, , `MySQL 5.1`, `Hive 0.7`,  and `Hive 0.10`. This document assumes you are using a Linux or Linux-like environment.

## Unit Test
Run unit test `test/unittest_legoo.py` for functional testing. Sample csv `census_population.csv` was used to load into `MySQL` and `Hive` table. User need to supply `Hive` and `MySQL` enviroment variables under section `setUp`

## Legoo Modules
To use `legoo` modules, you specify the module you want to use and the options that control the module. All modules ship with a help module. If error encountered or internal test failed, exception thrown, return_code set to 1, then control returned back to calling process. To display help with all avaialble options and sample usages, enter: `module_name -h` or `module_name --help` I will go over each of those modules briefly in turn.

### `csv_dump`
`csv_dump` is a `CSV` viewer with options for `--delimiter` and `--line_number`. It maps each field value to field name defined in header; print them out vertically with line number and column number. `csv_dump` allows user to dig into the middle of file with `--line_number` option. It is extremely handy to investigate data issues in a large `CSV` file having tons of fields.
##### `man page`:
    $ csv_dump -h
    Usage: csv_dump [options]
    Options:
      -h, --help                  show this help message and exit
      -d, --csv_delimiter         csv file delimiter, default: [tab]
      -l, --lines                 number of lines to dump out, default: [2]
      -n, --line_number           starting line number to dump out, default: [2]

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
      -h, --help                         show this help message and exit
      --mysql_ini                        mysql initial file for user, password and default db,default: [mysql.ini]
      --mysql_host                       target mysql host. default: [bidbs]
      --mysql_user                       mysql user, if not specified, get user from mysql_ini
      --mysql_password                   mysql password, if not specified, get password from mysql_ini
      --mysql_db                         target mysql database. default: [bi_staging]
      --mysql_table                      target mysql table name
      --mysql_create_table               mysql create table flag [Y|N]. default: [N]
      --mysql_truncate_table             mysql truncate table flag [Y|N]. default: [N]
      --csv_delimiter                    delimiter for csv file. default: [tab]
      --csv_header                       header flag for csv file. default: [Y]
      --csv_optionally_enclosed_by       csv_optionally enclosed_by for csv file
      --max_rows                         number of rows in csv file scanned to find column length
      -q --quiet --silent                OPTIONAL: suppress messages to stdout. default: [N]
      -d --debug                         OPTIONAL: debug flag [Y|N], default: [N]

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
      -h, --help                   show this help message and exit
      --hive_node                  target hive node. default: [namenode1]
      --hive_port                  target hive port number. default: 10000
      --hive_db                    target hive db. default: [staging]
      --hive_table                 hive table name. default: created from csv file name
      --hive_partition             partition name i.e. date_int=20130428
      --hive_create_table          CREATE_TABLE flag for hive table DDL. default: [Y]
      --hive_overwrite             OVERWRITE flag for hive table loading.default: [Y]
      --csv_header                 csv_header flag csv file default: [Y]
      --remove_carriage_return     remove carriage return from mysql source table. default: [N]
      --csv_delimiter              delimiter for csv file, default: [tab]
      -q --quiet --silent          suppress messages to stdout. default: [N]
      -d --debug                   debug flag [Y|N], default: [N]


##### example: generate `Hive` `DDL` from `CSV` header, create table based on `DDL`, then load data to table in `staging` db on `Hive` cluster . note: omitted options take on default value. For `CSV` haveing ~250 fields, imagine creating `MySQL DDL` yourself.

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
      -h, --help                 show this help message and exit
      --mysql_ini                mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host               mysql host for source data, default: [bidbs]
      --mysql_db         	     mysql database for source data, default: [bi]
      --mysql_user               mysql user, if not specified, get user from mysql_ini
      --mysql_password           mysql password, if not specified, get password from mysql_ini
      --mysql_quick              quick option for mysql client, default:[N]
      --mysql_table              mysql table to be exported
      --mysql_query              query results to be exported
      --hive_node                target hive node. default: [namenode1]
      --hive_port                target hive port. default: 10000
      --hive_db                  target hive db. default: [staging]
      --hive_ddl                 hive DDL for target hive table. default created from source mysql table
      --hive_create_table        CREATE_TABLE flag for hive table DDL. default: [N]
      --hive_overwrite           OVERWRITE flag for hive table loading. default: [Y]
      --hive_table               hive table name. default: created from csv file name
      --hive_partition           partition name i.e. date_int=20130428
      --remove_carriage_return   remove carriage return from mysql source table. default: [N]
      -q --quiet --silent        suppress messages to stdout. default: [N]
      -d --debug                 debug flag [Y|N], default: [N]


##### example: transfer query results from `MySQL` to `Hive` table `email_archive.fe_emailrecord_archive` `partition (date_key=20130710)` on `Hive` cluster. remove carriage return from the source data before transfer.

    $ mysql_to_hive --mysql_host='maildb-slave' --mysql_db='Email' --mysql_table='FE_EmailRecord' --mysql_query="select f.* from Email.FE_EmailRecord f where time_stamp > '2013-05-01' and time_stamp < '2013-05-02'" --hive_table='fe_emailrecord_archive' --hive_db='email_archive'  --remove_carriage_return='Y' --hive_partition="date_key=20130710"
    [INFO]  [mysql_to_csv][2013-11-18 10:06:08,580]:Running mysql export to csv ==>> [mysql -hmaildb-slave -uroot  Email  -e "select f.* from Email.FE_EmailRecord f where time_stamp > '2013-11-17' and time_stamp <= '2013-11-18'" > /data/tmp//FE_EmailRecord.csv]
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
      -h, --help                        show this help message and exit
      --hive_node                       source hive node. default: [namenode1]
      --hive_db                         source hive database. default: [staging]
      --hive_table                      source hive table name
      --hive_query                      free form query results to be exported
      --mysql_ini                       mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host                      target mysql host, default: [bidbs]
      --mysql_user                      mysql user, if not specified, get user from mysql_ini
      --mysql_password                  mysql password, if not specified, get password from mysql_ini
      --mysql_db                        target mysql database, default: [bi_staging]
      --mysql_table                     target mysql table name
      --mysql_create_table              mysql create table flag [Y|N], default: [N]
      --mysql_truncate_table            mysql truncate table flag [Y|N], default: [N]
      --csv_optionally_enclosed_by      enclosed_by for csv file
      --max_rows                        number of rows scanned to create mysql ddl
      -q --quiet --silent               suppress messages to stdout. default: [N]
      -d --debug                        debug flag [Y|N], default: [N]

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
      -h, --help               show this help message and exit
      --mysql_ini              mysql initial file for user, password and default db, default: [mysql.ini]
      --mysql_host             mysql host for source data, default: [bidbs]
      --mysql_db               mysql database for source data, default: [bi]
      --mysql_user             mysql user, if not specified, get user from mysql_ini
      --mysql_password         mysql password, if not specified, get password from mysql_ini
      --mysql_quick            mysql quick for large volume data
      --mysql_table            mysql table to be exported
      --mysql_query            query results to be exported
      --csv_dir                dir for csv file to be exported, default: [/data/tmp]
      --csv_file               the csv file to be exported, default: [table_name.csv]
      -q --quiet --silent      suppress messages to stdout. default: [N]
      -d --debug               debug flag [Y|N], default: [N]


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
      -h, --help             show this help message and exit
      --hive_node            source hive node. default: [namenode1]
      --hive_db              source hive database. default: [staging]
      --hive_table           source hive table name
      --hive_query           free form query results to be exported
      --csv_dir              dir for tsv
      --csv_file             export hive [table | query results] to tsv
      -q --quiet --silent    suppress messages to stdout. default: [N]
      -d --debug             debug flag [Y|N], default: [N]


##### example: export `Hive query result` to `TSV`: `/data/tmp/dim_time.csv`

    hive_to_csv --hive_node='namenode1' --hive_db='bi' --hive_table='dim_time' --hive_query='select * from dim_time limit 1000' --csv_dir='/data/tmp/'

##### example: export `Hive table` to `TSV`: `/data/tmp/dim_time2.csv`

    hive_to_csv --hive_node='namenode1' --hive_db='bi' --hive_table='dim_time' --csv_dir='/tmp/' --csv_file='dim_time2.csv'

## `execute_mysql_query`
`execute_mysql_query` run `mysql query` on remote `MySQL` server and return the results. 

##### `man page`:

    $ execute_mysql_query -h
    Usage: execute_mysql_query [options]

    Options:
      -h, --help                show this help message and exit
      --mysql_host              mysql host. default: [namehost1]
      --mysql_db                mysql db. default: [staging]
      --mysql_user              mysql user, if not specified, get user from mysql_ini
      --mysql_password          mysql password, if not specified, get password from mysql_ini
      --mysql_query             mysql query
      --row_count               row_count default: [N]
      -q --quiet --silent       suppress messages to stdout. default: [N]
      -d --debug                debug flag [Y|N], default: [N]

##### example: describe table [tmp_visit] on [bidbs].[bi_staging]
     
    execute_mysql_query --mysql_host='bidbs' --mysql_db='bi_staging' --mysql_user='root' --mysql_query='desc  tmp_visit'
    INFO      :[legoo][execute_mysql_query][2013-10-29 10:51:42,850]:running mysql query on [bidbs]:[bi_staging] ==>> [desc  tmp_visit]
    
    (('date_key', 'varchar(8)', 'YES', 'MUL', None, ''),
     ('email_type_id', 'int(11)', 'NO', '', '0', ''),
     ('visit_www', 'decimal(33,0)', 'YES', '', None, ''),
     ('visit_mobile', 'decimal(33,0)', 'YES', '', None, ''),
     ('visit_total', 'decimal(33,0)', 'YES', '', None, ''))

##### example: run table count [select count(*) from tmp_visit] on [bidbs].[bi_staging] and return tuple (rows_affected, number_of_rows)

    execute_mysql_query --mysql_host='bidbs' --mysql_db='bi_staging' --mysql_user='root' --mysql_query='select count(*) from tmp_visit' --row_count='Y'
    INFO      :[legoo][execute_mysql_query][2013-10-29 10:55:00,346]:running mysql query on [bidbs]:[bi_staging] ==>> [select count(*) from  tmp_visit]
    1
    467


##### example: drop table [tmp_visit] on [bidbs].[bi_staging]
 
    execute_mysql_query --mysql_host='bidbs' --mysql_db='bi_staging' --mysql_user='root' --mysql_query='drop table if exists tmp_visit'


## `execute_hive_query`
`execute_hive_query` run `hive query` on remote `Hive` server and return the results. 

##### `man page`:

    $ execute_hive_query -h
    Usage: execute_hive_query [options]

    Options:
      -h, --help                           show this help message and exit
      --hive_node                          hive node. default: [namenode2s]
      --hive_port                          hive port number. default: 10000
      --hive_db                            hive db. default: [staging]
      --hive_query                         hive query
      --mapred_job_priority                map reduce job priority [VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW]. default: [NORMAL]
      -q --quiet --silent                  suppress messages to stdout. default: [N]
      -d --debug                           debug flag [Y|N], default: [N]
      
##### example: describe hive table [top50_ip] on [namenode2s]:[staging]: 
     
    execute_hive_query --hive_node='namenode2s' --hive_db='staging' --hive_query='desc top50_ip'
    INFO:[legoo][execute_remote_hive_query][2013-10-29 11:35:02,448]:running hive query on [namenode2s]:[staging] ==>> [desc top50_ip]
    
    date_key		string	
    hour		    string	
    site		    string	
    ip_addr		    string	
    domain_info		string	
    url_part		string	
    cnt_over_2500	string	


##### example: sample hive table [top50_ip] on [namenode2s]:[staging]:


    execute_hive_query --hive_node='namenode2s' --hive_db='staging' --hive_query='select * from top50_ip limit 10'
    INFO      :[legoo][execute_remote_hive_query][2013-10-29 11:35:31,117]:running hive query on [namenode2s]:[staging] ==>> [select * from top50_ip limit 10]
    
    20131029	4	api	-	unable to determine	NULL	5712
    20131029	5	api	-	unable to determine	NULL	5750
    20131029	6	api	-	unable to determine	NULL	5709
    20131029	7	api	-	unable to determine	NULL	5748
    20131029	8	api	-	unable to determine	NULL	5726
    20131029	8	api	174.234.1.176	176.sub-174-234-1.myvzw.com	tma	2516
    20131029	4	www	192.254.78.136	NetRange:       192.254.78.128 - 192.254.78.159	blog	3839
    20131029	5	www	192.254.78.136	NetRange:       192.254.78.128 - 192.254.78.159	blog	3150
    20131029	6	www	192.254.78.136	NetRange:       192.254.78.128 - 192.254.78.159	blog	3819
    20131029	7	www	192.254.78.136	NetRange:       192.254.78.128 - 192.254.78.159	blog	3808
    

      
## `wait_for_file`
`wait_for_file` check if file exists and modified time after `mtime_after`.  if not, retry based on `sleep_interval`, `num_retry`, `stop_at`

##### `man page`:
    $ ./wait_for_file -h
    Usage:
      check if file exists and mtime after [mtime_after].
      if not, retry based on [sleep_interval], [num_retry], [stop_at]
      =================================================================================
      wait_for_file -s 10 -a '2013-10-10 13:44' -n 20 -f test.txt -m '2013-10-10 13:51'
      =================================================================================

    Options:
      -h, --help            show this help message and exit
      -s --sleep_interval   sleep for approximately s seconds between iterations. default: [60]
      -n --num_retry        number of retry
      -m --mtime_after      file modified after datetime [yyyy-mm-dd hh:mm]. i.e. [2013-10-08 14:30]
      -a --stop_at          stop checking file at datetime [yyyy-mm-dd hh:mm]. i.e. [2013-10-08 15:30]
      -f --file             file name
      -q --quiet --silent   suppress messages to stdout. default: [N]
      -d --debug            debug flag [Y|N], default: [N]

## `wait_for_table`
`wait_for_table` check if table exists and has updated after `update_after`. otherwise, retry based on `sleep_interval`, `num_retry` and/or `stop_at`

##### `man page`:
    $ ./wait_for_table -h
    Usage:
      check if table exists and has updated after [update_after]
      if not, retry based on [sleep_interval], [num_retry] and/or [stop_at]
      NOTE:
      1. need access to INFORMATION_SCHEMA.TABLES to retrieve update_time
      2. option [stop_at] i.e. [2013-10-08 15:30], together with [update_after]
         i.e. [2013-10-09 14:25], define the table wait window
      3. option [ETL_TABLE] and [ETL_JOB] are trulia specific which retrive table last update
         from proprietary [AUDIT_JOB] database
      ======================================================
      ./wait_for_table --mysql_table='tmp_dim_property' --update_after='2013-10-10 16:07' --sleep_interval=10 --num_retry=20 --stop_at='2013-10-10 16:12' --debug='Y'
      ======================================================

    Options:
      -h, --help            show this help message and exit
      --mysql_host          target mysql host. default: [bidbs]
      --mysql_db            target mysql database. default: [bi_staging]
      --mysql_user          mysql user
      --mysql_password      mysql password
      --mysql_table         mysql table name
      --update_after        mysql table modified after datetime [yyyy-mm-dd hh:mm] i.e. [2013-10-09 14:25]
      --etl_table           mysql table name. WARNING: TRULIA proprietary
      --etl_job             job name. WARNING: TRULIA proprietary
      -s --sleep_interval   sleep for approximately s seconds between iterations. default: [60]
      -n --num_retry        number of retry
      -a --stop_at          stop checking file at datetime [yyyy-mm-dd hh:mm]. i.e. [2013-10-08 15:30]
      -q --quiet --silent   suppress messages to stdout. default: [N]
      -d --debug            debug flag [Y|N], default: [N]

## `qa_mysql_table`
Instead of expensive and unwieldy `profiling`, `qa_mysql_table` takes lightweight approach to check `MySQL` table after `etl`.

##### `man page`:
    $ qa_mysql_table -h
    Usage:

      Instead of expensive and unwieldy profiling, qa_mysql_table takes super lightweight approach 
      to check mysql table after etl:
      compare mysql query result counts to threshhold_value using comparison_operator
      ======================================================================================
      # check if:  1) one row created 2) interest_rate > 0 for 20131027 build
      qa_mysql_table --mysql_db='bi' --mysql_host='bidbs' --mysql_user='root'
      --mysql_query='select count(*) from fact_mortgage_rate where interest_rate > 0 and date_key = 20131027'
      --comparison_operator='=' --threshhold_value=1
      ======================================================================================
    
      ======================================================================================
      # check if each colunm populated which has different source for 20131027 build.
      # expand the where clause if necessary
      qa_mysql_table --mysql_db='bi' --mysql_host='bidbs' --mysql_user='root'
      --mysql_query='select count(*) from agg_zip_stat_daily where date_key=20131027 and zip=94103
      and for_sale_median_price > 0 and num_listing >0 and pdp_android_property_view_cnt 
      --comparison_operator='>' --threshhold_value=1
      ======================================================================================

      ======================================================================================
      # for monster table, check if partition populated with expected rows
      # fact_property_view_anonymous has 3 billions rows partitioned on date_key
      # check if partition 20131027 populated with more than 2 million rows
      qa_mysql_table --mysql_db='bi' --mysql_host='bidbs' --mysql_user='root'
      --mysql_query='select count(*) from fact_property_view_anonymous where date_key = 20131027
      --comparison_operator='>' --threshhold_value=2000000
      ======================================================================================
      
    Options:
      -h, --help                    show this help message and exit
      --mysql_host                  mysql host. default: [bidbs]
      --mysql_db                    mysql db. default: [staging]
      --mysql_user                  mysql user, if not specified, get user from mysql_ini
      --mysql_password              mysql password, if not specified, get password from mysql_ini
      --mysql_query                 mysql query for QA
      --comparison_operator         comparison_operator [=, ==, >, >=, <, <=, <>, !=] to compare [query result] to [threshhold_value]
      --threshhold_value            threshhold_value
      -q --quiet --silent           suppress messages to stdout. default: [N]
      -d -debug                     debug flag [Y|N], default: [N]

##### example: check if partition `20131027` populated with 2+ millon rows in 3 billion rows table `fact_property_view_anonymous`.  
    
    time qa_mysql_table --mysql_db='bi' --mysql_host='bidbs' --mysql_user='root'   --mysql_query='select count(*) from fact_property_view_anonymous where date_key = 20131027' --comparison_operator='>' --threshhold_value=2000000 
    INFO      :[legoo][execute_mysql_query][2013-10-29 14:30:54,776]:running mysql query on [bidbs]:[bi] ==>> [select count(*) from fact_property_view_anonymous where date_key = 20131027]
    INFO      :[legoo][qa_mysql_table][2013-10-29 14:30:55,361]:[INFO] [select count(*) from fact_property_view_anonymous where date_key = 20131027] passed test: {[2882881] [>] [2000000]}
    
    real      0m0.639s
    user      0m0.057s
    sys	      0m0.010s
    
## `send_mail`
send `email` in `plain` or `html` format, and attach `files` from list or `dir`


##### `man page`:
    $ send_mail -h
    
    Usage: 
      send email in plain or html format, and attach files from list or dir
      ==================================================================================================================================================================
      send_mail  --sender='luo@trulia.com' --subject='legoo email' --receivers='pluo@trulia.com' --body_html_file='/home/dataproc/bar.html' --attachment_files='csv_dump'
      ===================================================================================================================================================================
  
    Options:
        -h, --help                show this help message and exit
        --smtp_server             MANDATORY: smtp server name. default: [mx1.sv2.trulia.com]
        --smtp_port               smtp port number. default: [25]
        --sender                  MANDATORY: sender eamil
        --receivers               MANDATORY: list of email recipients. i.e. 'a@xyz.com, b@xyz.com'
        --subject                 email subject
        --body_text               OPTIONAL: text as email body
        --body_text_file          OPTIONAL: file used as email body
        --body_html               OPTIONAL: html as email body
        --body_html_file          OPTIONAL: html file used as email body
        --attachment_files        OPTIONAL: list of files as attachment
        --attachment_dir          OPTIONAL: attach all files (not recursively) in directory
        -q, --quiet, --silent     OPTIONAL: suppress messages to stdout. default: [N]
        -d --debug                OPTIONAL: debug flag [Y|N], default: [N]
    
## Future Release
* option `dry_run`
* option `escape_hive_keyword` to escape hive keywords 
* option `config_file` for unit testing
* `Apache HBase` client
* more ...

## Contributor
* Patrick Luo ([partick.luo2006@gmail.com]())

## License
[Legoo is licensed under the MIT License](https://github.com/trulia/legoo/blob/master/LICENSE.txt)
