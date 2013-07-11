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

### csv_dump 
`csv_dump` is a `CSV` viewer with options for `--delimiter` and `--line_number`. It maps each field value to field name defined in header, print them out vertically with line number and column number. `csv_dump` allows user to dig into the middle of file with `--line_number` option. It is extremely handy to investigate data issues in a large `CSV` file having tons of fields. 
##### display help: `csv_dump -h` 
    Usage: csv_dump [options]
    Options:
      -h, --help                                          show this help message and exit
      -d CSV_DELIMITER, --csv_delimiter=CSV_DELIMITE      csv file delimiter, default: [tab]
      -l LINES, --lines=LINES                             number of lines to dump out, default: [2]
      -n LINE_NUMBER, --line_number=LINE_NUMBER           starting line number to dump out, default: [2]
##### example: pretty print for line 505 `csv_dump -d 'tab' -n 505 /data/tmp/dim_listing_delta.csv` 
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
    ..........

## csv_to_mysql 
csv_to_mysql load csv file to target MySQL with options --mysql_create_table, --mysql_truncate_table, --csv_delimiter, --csv_header, --csv_optionally_enclosed_by etc. when --mysql_create_table set to 'Y', DDL generated from csv header, column length calculated by scanning the file, and finally run the DDL on target MySQL server to create table. Non zero RC returned and exception raised for errors like csv count not match table count

## csv_to_hive: 
csv_to_hive load csv file to remote target Hive cluster. When option --hive_create_table = 'Y', DDL generated from csv header to create hive table. 

## mysql_to_hive: 
mysql_to_hive transfer data from MySQL table or query result to Hive table with options: --mysql_quick, --mysql_table, --mysql_query, --hive_db, --hive_create_table, --hive_overwrite, --hive_table, --hive_partition, --remove_carriage_return etc. When --hive_create_table = 'Y', hive table created based on MySQL DDL. For large data transfer with more than 1 million rows, set option --mysql_quick to use less resource on MySQL server. option --hive_partition allows user to transfer data directly to hive partition. --remove_carriage_return removes carriage return from the MySQL source. For column name key word clashes, column name postfix with _new. Non zero RC returned and exception raised if validation failed such as row count not match from MySQL to Hive. 

## hive_to_mysql: 
hive_to_mysql move data from Hive table or query result to MySQL table. When --mysql_create_table set to 'Y', MySQL table created based on Hive table DDL. option --mysql_truncate_table allows use to truncate MySQL table first before loading. Non zero RC returned and exception raised if validation failed such as row count not match from MySQL to Hive. 

## mysql_to_csv 
## hive_to_csv 
export table/query results from MySQL/Hive to tsv. 


To conclude, legoo is a general purpose tool to transfer data among csv, MySQL and Hive. It can be easily extened with more modules like HBase (work in progress). I had fun building and playing legoo and I hope you do too. 


## License
[Legoo is licensed under the MIT License](https://github.com/trulia/hologram/blob/master/LICENSE.txt)
