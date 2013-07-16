Introducing `Legoo`: A developer tool for data transfer among `CSV`, `MySQL`, and `Hive` (`HBase` incubating)
=====

`legoo` is a collection of modules to automate data transfer among `CSV`, `MySQL`, and `Hive`. It's written in `Python` and provides ease of programming, flexibility, transparency, and extensibility. 

legoo is available on GitHub (https://github.com/trulia/legoo) 

## The backstory: 

There are tools already such as `MySQL` LOAD INFILE, [Sqoop](http://sqoop.apache.org) etc. Then why reinvent the wheel? 

I refer `CSV` as plain text file with delimiters such as comma, tab, etc. 

Let me start with `MySQL` LOAD INFILE limitations that load `CSV` into `MySQL`. First, target table must be pre defined. This would be a challenge if there were a lot of fields in `CSV`. For instance, we have `CSV` from salesforce having 200+ columns. Creating `MySQL` DDL with appropriate column length is frustrating to say the least; second, `CSV` must be local on `MySQL` server; Third, lacking of verification such as `CSV` line count to table count; 

To transfer data between Hadoop/`Hive` and `MySQL`, [Sqoop](http://sqoop.apache.org) from [Cloudera](http://www.cloudera.com) is the best tool available. However, as of 4/1/2013, the performance did not meet my expectations; It crashed when there is carriage return or hive keywords (i.e. location) in `MySQL` DDL; User can not set the `Hive` cluster dynamically; It can not create `MySQL` table dynamically when export `Hive` table to `MySQL`; Nor support `CSV` to `Hive`; only connect to `Hive` DB default; the list goes on and on and on and on. 

Out of frustration, I built legoo during the [trulia](http://www.trulia.com) innovation week! 

For ease of programming, I created modules,  which are wrapper scripts with python function call. Here is the high level view of modules. I will go over each of those modules briefly in turn.

![diagram](https://raw.github.com/trulia/legoo/master/modules.jpg?login=pluo-trulia&token=974a2a8c87eb001d1219ab09e1794b18 "module diagram")

### csv_dump 
csv_dump is a `CSV` viewer with options for --delimiter and --line_number. It maps each field value to field name defined in header; print them out vertically with line number and column number. csv_dump allows user to dig into the middle of file with --line_number option. It is extremely handy to investigate data issues in a large `CSV` file having tons of fields. 

### csv_to_mysql
csv_to_mysql load `CSV` to target `MySQL` table with options --mysql_create_table, --mysql_truncate_table, --csv_delimiter, --csv_header, --csv_optionally_enclosed_by etc. when --mysql_create_table set to 'Y', DDL generated from `CSV` header, column length calculated by scanning the file, and finally run the DDL on target `MySQL` server to create table. Non zero RC returned and exception raised for errors like `CSV` count not match table count

### csv_to_hive 
csv_to_hive load `CSV` into remote `Hive` cluster. When option --hive_create_table set to 'Y', DDL generated from `CSV` header to create `Hive` table. 

### mysql_to_hive 
mysql_to_hive transfer data from `MySQL` table or query result to `Hive` table with options: --mysql_quick, --mysql_table, --mysql_query, --hive_db, --hive_create_table, --hive_overwrite, --hive_table, --hive_partition, --remove_carriage_return etc. When --hive_create_table set to 'Y', `Hive` table created based on `MySQL` DDL. For large data transfer with millions rows, set option --mysql_quick to 'Y' to avoid buffering on `MySQL` server. option --hive_partition allows user to transfer data directly to `Hive` partition. --remove_carriage_return removes carriage return from the `MySQL` source. For column name key word clashes, column name postfix with _new automatically. Non zero RC returned and exception raised if validation failed such as row count not match from `MySQL` to `Hive`. 

### hive_to_mysql 
hive_to_mysql transfer data from `Hive` table or `Hive` query result to `MySQL` table. When --mysql_create_table set to 'Y', `MySQL` table created based on `Hive` table DDL. option --mysql_truncate_table allows user to truncate `MySQL` table first before loading. Non zero RC returned and exception raised if validation failed such as row count not match from `MySQL` to `Hive`. 

### mysql_to_csv
mysql_to_csv export `MySQL` table or `MySQL` query result to TSV. 

### hive_to_csv 
hive_to_csv export `Hive` table or `Hive` query result to TSV. 

## Check it out from Github
you can check it out on our github page: https://github.com/trulia/legoo

