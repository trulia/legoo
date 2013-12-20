#!/usr/bin/python

# Patrick Luo
# innovation week of Mar 25, 2013
# objective: high performance, easy to use and maintain, flexibility to run anywhere on any DB (HIVE and MySQL), etc.

import os, sys, subprocess, inspect
import MySQLdb      # MySQL DB module
import ConfigParser # parse mysql ini file
import csv          # csv module for csv parsing
import logging
import datetime
from   optparse import OptionParser
from   time import sleep
from   pprint import pprint # pretty print for dictionary etc
import operator

# logging config
logging.basicConfig(
  filename =  os.path.dirname(os.path.realpath(__file__)) + '/legoo.log',   # set log file to legoo directory
  format   = "%(levelname)-10s:[%(module)s][%(funcName)s][%(asctime)s]:%(message)s",
  level    = logging.INFO
)
format = logging.Formatter("%(levelname)-10s:[%(module)s][%(funcName)s][%(asctime)s]:%(message)s")

# create a handler for stdout
info_hand = logging.StreamHandler(sys.stdout)
info_hand.setLevel(logging.INFO)
info_hand.setFormatter(format)

# top-level logger print to file
legoo = logging.getLogger("legoo")
legoo.addHandler(info_hand)

# add hive path
hive_path='/usr/lib/hive/lib/py/'
if hive_path not in sys.path:
      sys.path.insert(0, hive_path)

trulia_mysql_host = ['bidbs', 'bidbm', 'bedb1', 'maildb-slave', 'db30', 'rodb-dash', 'db9', 'crad103']

def count_lines(**kwargs):
  """return line count for input file
  --------------------------------------------------------------------
  count_lines(file='/tmp/msa.csv', skip_header='Y')
  --------------------------------------------------------------------
  """
  debug       = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs

  # dictionary initialized with the name=value pairs in the keyword argument list
  file        = kwargs.pop("file", None)
  skip_header = kwargs.pop("skip_header", 'N' ) # flag to skip header
  quiet       = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))  # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  p = subprocess.Popen(['wc', '-l', file],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
  result, err = p.communicate()
  if p.returncode != 0:
    raise IOError(err)
  num_lines = int(result.strip().split()[0])
  # decrement num_lines if need to skip header
  if (skip_header.strip().lower() == 'y'):
    num_lines = num_lines - 1

  legoo.info("[%s] line count ==>> [%s] lines" % (file, num_lines))
  return num_lines

def remove_file(**kwargs):
  """remove file
  ---------------------------------
  count_lines(file='/tmp/msa.csv')
  ---------------------------------
  |  file        = None
  |  debug       = N
  """
  debug       = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)

  # dictionary initialized with the name=value pairs in the keyword argument list
  file        = kwargs.pop("file", None)
  quiet       = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))
  p = subprocess.Popen(['rm', '-f', file],
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
  result, err = p.communicate()
  if p.returncode != 0:
    raise IOError(err)

  legoo.info('File [%s] removed' % (file))

def count_hive_table_rows (**kwargs):
  """return hive table row count
  -----------------------------------------------------------------------------------
  count_hive_table_rows(hive_node='namenode2s', hive_db='bi', hive_table='dual')
  -----------------------------------------------------------------------------------
  """

  # dictionary initialized with the name=value pairs in the keyword argument list
  debug               = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  hive_node           = kwargs.pop("hive_node", "namenode2s")
  hive_port           = kwargs.pop("hive_port", 10000)
  hive_db             = kwargs.pop("hive_db", "staging")
  hive_table          = kwargs.pop("hive_table", None)
  mapred_job_priority = kwargs.pop("mapred_job_priority", "NORMAL")
  quiet               = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  rs = execute_remote_hive_query( hive_node = hive_node, hive_port = hive_port, \
                                  hive_db = hive_db, mapred_job_priority = mapred_job_priority, \
                                  hive_query = "SELECT count(*) from %s" % (hive_table))
  table_rows = rs[0]
  legoo.info('[%s] row count ==>> [%s] rows' % (hive_table, table_rows))

  return table_rows

def mysql_to_hive(**kwargs):
  """dump [mysql table | mysql query results] to hive
  ------------------------------------------------------------------------------
  mysql_to_hive(mysql_host='bidbs', mysql_table='dim_time', hive_create_table='Y')
  ------------------------------------------------------------------------------
  """
  debug                  = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # args for mysql_to_csv
  mysql_ini              = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host             = kwargs.pop("mysql_host", "bidbs")
  mysql_db               = kwargs.pop("mysql_db", "bi")
  mysql_user             = kwargs.pop("mysql_user", "root")
  mysql_quick            = kwargs.pop("mysql_quick", "N")
  mysql_table            = kwargs.pop("mysql_table", None)
  mysql_query            = kwargs.pop("mysql_query", None)
  mysql_password         = kwargs.pop("mysql_password", None)
  # args for csv_to_mysql
  hive_node              = kwargs.pop("hive_node", "namenode2s")
  hive_port              = kwargs.pop("hive_port", 10000)
  hive_db                = kwargs.pop("hive_db", "staging")
  hive_table             = kwargs.pop("hive_table", None)
  hive_partition         = kwargs.pop("hive_partition", None)
  hive_ddl               = kwargs.pop("hive_ddl", None)
  hive_overwrite         = kwargs.pop("hive_overwrite", "Y")
  hive_create_table      = kwargs.pop("hive_create_table", "N")
  mapred_job_priority    = kwargs.pop("mapred_job_priority", "NORMAL")
  csv_dir                = kwargs.pop("csv_dir", "/data/tmp/")
  csv_file               = kwargs.pop("csv_file", None)
  csv_delimiter          = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  remove_carriage_return = kwargs.pop("remove_carriage_return", 'N')
  quiet                  = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # export mysql to csv
  csv_file = mysql_to_csv( mysql_ini      = mysql_ini, \
                           mysql_host     = mysql_host, \
                           mysql_db       = mysql_db, \
                           mysql_user     = mysql_user, \
                           mysql_password = mysql_password, \
                           mysql_quick    = mysql_quick, \
                           mysql_table    = mysql_table, \
                           mysql_query    = mysql_query, \
                           csv_dir        = csv_dir, \
                           csv_file       = csv_file, \
                           quiet          = quiet, \
                           debug          = debug)
  # load csv to hive
  csv_to_hive(hive_node                   = hive_node, \
              hive_port                   = hive_port, \
              hive_db                     = hive_db, \
              hive_create_table           = hive_create_table, \
              hive_table                  = hive_table, \
              hive_overwrite              = hive_overwrite, \
              hive_partition              = hive_partition, \
              hive_ddl                    = hive_ddl, \
              mapred_job_priority         = mapred_job_priority, \
              csv_file                    = csv_file, \
              csv_delimiter               = csv_delimiter, \
              remove_carriage_return      = remove_carriage_return, \
              quiet                       = quiet, \
              debug                       = debug)
  # remove temp files
  remove_file(file=csv_file)
  # temp files if remove_carriage_return is on
  remove_file(file="%s%s" % (csv_file, '2'))

def mysql_to_csv(**kwargs):
  """export [mysql table | query results] to tab delmited csv and return the tsv
  -------------------------------------------------------------------------------
  mysql_to_csv(mysql_host='bidbs', mysql_table='dim_time')
  mysql_to_csv(mysql_host='bidbs', mysql_query='select * from dim_time limit 10')
  -------------------------------------------------------------------------------
  """
  debug     = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_ini       = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host      = kwargs.pop("mysql_host", "bidbs")
  mysql_db        = kwargs.pop("mysql_db", "bi")
  mysql_user      = kwargs.pop("mysql_user", "root")
  mysql_password  = kwargs.pop("mysql_password", None)
  mysql_quick     = kwargs.pop("mysql_quick", "N")
  mysql_table     = kwargs.pop("mysql_table", None)
  mysql_query     = kwargs.pop("mysql_query", None)
  csv_dir         = kwargs.pop("csv_dir", "/data/tmp/")
  csv_file        = kwargs.pop("csv_file", None)
  csv_delimiter   = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  quiet           = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # parse the ini file to pull db variables
  config = ConfigParser.ConfigParser()
  config.read(os.path.join(os.path.dirname(__file__), mysql_ini))

  # extend e mysql.ini if necessary
  # set default mysql_user from mysql_ini
  if (not mysql_user):
    if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db.
      mysql_user = config.get(mysql_host, "user")
    else:
      mysql_user = config.get('default', "user")

  if (not mysql_db):
    if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db.
      mysql_db = config.get(mysql_host, "db")
    else:
      mysql_db = config.get('default', "db")

  # set default mysql_password from mysql_ini
  if (not mysql_password):
    if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db.
      mysql_password = config.get(mysql_host, "password")
    else:
      mysql_password = config.get('default', "password")

  # set default csv
  if (not csv_file and not mysql_table ):
    csv_file = "%s/tmp_%s.csv" % (csv_dir, os.getpid()) # set a temporary csv file name in
  elif (not csv_file and mysql_table ):
    csv_file = "%s/%s.csv" % (csv_dir, mysql_table)

  if (not mysql_table and not mysql_query):
    raise TypeError("[ERROR] Must specify either mysql_table or mysql_query" )
  elif (mysql_table and not mysql_query):
    mysql_query = "SELECT * FROM %s;" % (mysql_table)

  if (mysql_quick and mysql_quick.strip().lower() == 'y'):
    mysql_quick = "--quick"
  else:
    mysql_quick = ""

  # mysql -hbidbs bi -e'select * from dim_time limit 10' > /tmp/test.csv
  mysql_cmd = "mysql -h%s -u%s -p%s %s %s -e \"%s\" > %s" % \
              (mysql_host, mysql_user, mysql_password, mysql_db, mysql_quick, mysql_query, csv_file)
  mysql_cmd_without_password = "mysql -h%s -u%s  %s %s -e \"%s\" > %s" % \
              (mysql_host, mysql_user, mysql_db, mysql_quick, mysql_query, csv_file)
  legoo.info("Running mysql export to csv ==>> [%s]" % ( mysql_cmd_without_password))

  os.system( mysql_cmd )
  if (debug.strip().lower() == 'y'):
    # dump sample
    csv_dump(csv_file=csv_file, csv_delimiter=csv_delimiter, lines=2)
  return csv_file

def csv_to_hive(**kwargs):
  """import csv to to hive table.
  1. create hive ddl base on csv header. use octal code for csv_delimiter
  2. create hive table
  3. upload csv without header to hdfs
  4. load csv in hdfs to hive table
  note: sqoop is buggy, many mandatory parameters, only run on hive node and other restrictions.
  -----------------------------------------------------------------------------------
  csv_to_hive(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', hive_create_table='Y')
  -----------------------------------------------------------------------------------
  """
  debug                  = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  hive_node              = kwargs.pop("hive_node", "namenode2s")
  hive_port              = kwargs.pop("hive_port", 10000)
  hive_db                = kwargs.pop("hive_db", "staging")
  hive_table             = kwargs.pop("hive_table", None)
  hive_ddl               = kwargs.pop("hive_ddl", None)
  hive_overwrite         = kwargs.pop("hive_overwrite", "Y")
  hive_create_table      = kwargs.pop("hive_create_table", "N")
  hive_partition         = kwargs.pop("hive_partition", None)
  mapred_job_priority    = kwargs.pop("mapred_job_priority", "NORMAL")
  csv_file               = kwargs.pop("csv_file", None)
  csv_header             = kwargs.pop("csv_header", "Y")
  remove_carriage_return = kwargs.pop("remove_carriage_return", "N")
  csv_delimiter          = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  quiet                  = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  if (not hive_table):
    legoo.error("hive_table variable need to specified")    # log error
    raise TypeError("[ERROR] hive_table variable need to specified")

  # if not hive_overwrite, set to append mode
  if (hive_overwrite.strip().lower() == 'y'):
    hive_overwrite = 'OVERWRITE'
  else:
    hive_overwrite = 'INTO'

  # When input file has no header, create temp file with dummy header
  if (csv_header.strip().lower() <> 'y'):
    legoo.info("audto generating csv header ...")
    temp_file = csv_file + '2'
    # read first line of file
    with open(csv_file, 'r') as f:
      first_line = f.readline()
    # autogen column name list
    if (csv_delimiter.strip().lower() == 'tab'):
      raw_delimiter = '\t'
    else:
      raw_delimiter = csv_delimiter

    header_list = [ 'col_' + str(i) for i in range(len(first_line.split(raw_delimiter)))]
    # write header to temp file
    with open(temp_file,'w') as f:
      wr = csv.writer(f,  delimiter=raw_delimiter, quoting=csv.QUOTE_ALL, lineterminator='\n')
      wr.writerow(header_list)

    cmd_append = 'cat %s >> %s' % (csv_file, temp_file) # cmd to append file
    os.system( cmd_append )  # os.system call is easier than subprocess
    csv_file = temp_file
    csv_header = 'Y'

  # chcek table if exists on hive
  if (hive_create_table.strip().lower() == 'n'):
    execute_remote_hive_query( hive_node = hive_node, hive_port = hive_port, \
                               hive_db = hive_db, mapred_job_priority = mapred_job_priority, \
                               hive_query = "desc %s" % (hive_table))

  if (hive_partition and hive_create_table.strip().lower() == 'y'):
    hive_create_table ='N'
    legoo.warning("hive_create_table can not set together with hive_partition. reset hive_create_table = N")

  # create hive staging table ddl based on csv header, then create hive staging table
  (filename, extension) =  os.path.splitext(os.path.basename(csv_file))
  hive_staging_table = "tmp_legoo_%s" % (os.getpid())
  # replace . with _ in table name
  # hive_staging_table = hive_staging_table.replace('.', '_')

  # create staging table ddl
  (hive_staging_table, hive_ddl) = create_hive_ddl_from_csv(csv_file      = csv_file, \
                                                            csv_delimiter = csv_delimiter, \
                                                            table_name    = hive_staging_table, \
                                                            quiet         = quiet, \
                                                            debug         = debug)
  # drop staging table if exists
  execute_remote_hive_query(hive_node  = hive_node, hive_port = hive_port, \
                            hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                            quiet      = quiet, debug = debug, \
                            hive_query = "DROP TABLE IF EXISTS %s" % (hive_staging_table))
  # create empty table
  execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                             hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                             quiet      = quiet, debug = debug, \
                             hive_query = hive_ddl)

  # load csv to hive staging table
  csv_to_hive_table(hive_node              = hive_node, \
                    hive_port              = hive_port, \
                    hive_db                = hive_db, \
                    hive_table             = hive_staging_table, \
                    hive_overwrite         = hive_overwrite, \
                    mapred_job_priority    = mapred_job_priority, \
                    csv_file               = csv_file, \
                    csv_delimiter          = csv_delimiter, \
                    csv_header             = csv_header, \
                    remove_carriage_return = remove_carriage_return, \
                    quiet                  = quiet, \
                    debug                  = debug)
  # example: hive_partition="date_int = 20130428"
  if (hive_partition):
    if (not hive_table):
      legoo.error("hive_table need to specified")
      raise TypeError("[ERROR] hive_table need to specified")
    hive_query = "ALTER TABLE %s DROP IF EXISTS PARTITION (%s)" % (hive_table, hive_partition)
    execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                               hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                               quiet      = quiet, debug = debug, \
                               hive_query = hive_query)
    hive_query = "ALTER TABLE %s ADD PARTITION (%s)" % (hive_table, hive_partition)
    execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                               hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                               quiet      = quiet, debug = debug, \
                               hive_query = hive_query)
    # load staging table to target table
    hive_query = "INSERT OVERWRITE TABLE %s partition (%s) select * from %s" % (hive_table, hive_partition, hive_staging_table)
  elif (hive_create_table.strip().lower() == 'y'):
    hive_query = "ALTER TABLE %s RENAME TO %s" % (hive_staging_table, hive_table)
  elif (hive_create_table.strip().lower() == 'n'):
    hive_query = "INSERT %s TABLE %s select * from %s" % (hive_overwrite, hive_table, hive_staging_table)
  execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                             hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                             quiet      = quiet, debug = debug, \
                             hive_query = hive_query)

  # drop staging table
  hive_query = "DROP TABLE IF EXISTS %s" % (hive_staging_table)
  execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                             hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                             quiet      = quiet, debug = debug, \
                             hive_query = hive_query)
  if (hive_partition):
    partition_str = "PARTITION (%s)" % hive_partition
  else:
    partition_str = ""
  legoo.info("hive table [%s]:[%s].[%s] %s successfully built" % (hive_node, hive_db, hive_table, partition_str))
  # check if temp file exists and remove
  try:
      temp_file
  except NameError:
      pass
  else:
      remove_file(file=temp_file) # remove temp file

def csv_to_hive_table(**kwargs):
  """import csv to to existing hive table.
  1. upload csv without header to hdfs
  2. load csv from hdfs to target hive table
  note:
  1. sqoop is slow, buggy, can't handle hive keywords, special character in input, etc
  2. Two approaches to load into partition table. first approach is load from table. the other
  approach use load infile which is more efficient but input file must have the same format
  i.e. file type, csv_delimiter etc as target table definition. To make the tool more elastic and
  more fault tolerant, first approach is choosen.
  -----------------------------------------------------------------------------------
  csv_to_hive_table(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', hive_create_table='Y')
  -----------------------------------------------------------------------------------
  """
  debug     = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  hive_node              = kwargs.pop("hive_node", "namenode2s")
  hive_port              = kwargs.pop("hive_port", 10000)
  hive_db                = kwargs.pop("hive_db", "staging")
  hive_table             = kwargs.pop("hive_table", None)
  hive_overwrite         = kwargs.pop("hive_overwrite", "Y")
  mapred_job_priority    = kwargs.pop("mapred_job_priority", "NORMAL")
  csv_file               = kwargs.pop("csv_file", None)
  csv_header             = kwargs.pop("csv_header", "Y")
  csv_delimiter          = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  remove_carriage_return = kwargs.pop("remove_carriage_return", "N")
  quiet                  = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # raise exception if partition specified but table name not specified
  if (not hive_table):
    legoo.error("Table [%s] must specified!" % (hive_table))

  # trulia specific: ssh login to cdh4 cluster namenode2s as user dwr
  if (hive_node.strip().lower().split('.')[0] == 'namenode2s'):
    ssh_hive_node = 'dwr@' + hive_node
  else:
    ssh_hive_node = hive_node

  # remove the carriage return from input csv file
  if (remove_carriage_return.strip().lower() == 'y'):
    temp_file = csv_file + '2'
    cmd_remove_carriage_return = 'tr -d \'\\r\'  < ' +  csv_file + ' > ' + temp_file # replace carriage return with #
    legoo.info("remove special chracter \\ with # ==>> [%s]" % (cmd_remove_carriage_return))
    os.system( cmd_remove_carriage_return )  # os.system call is easier than subprocess
    csv_file = temp_file

  if (debug.strip().lower() == 'y'):
    # dump the first 2 lines to verify
    csv_dump(csv_file=csv_file, csv_delimiter=csv_delimiter, lines=2)

  hdfs_inpath = "/tmp/" + hive_table   # set hdfs_inpath
  # hadoop will not overwrite a file - so we'll nuke it ourselves
  hdfs_cmd = "ssh %s \'. .bash_profile; hadoop fs -rm %s 2>/dev/null\'" % (ssh_hive_node, hdfs_inpath)
  legoo.info("running hdfs clean up ==>> [%s]" % ( hdfs_cmd))
  os.system( hdfs_cmd )  # os.system call is easier than subprocess for |
  # upload csv to hdfs. - for stdin, skip header
  if (csv_header.strip().lower() == 'y'):
    skip_header = 2
  else:
    skip_header = 1
  hdfs_cmd = "tail -n +%d %s | ssh %s \'hadoop fs -put - %s\'" % (skip_header, csv_file, ssh_hive_node, hdfs_inpath)
  legoo.info("running csv upload to hdfs ==>> [%s]" % ( hdfs_cmd))
  os.system( hdfs_cmd )  # os.system call is easier than subprocess for |
  # load data inpath '/tmp/fact_imp_pdp.csv' overwrite into table tmp_fact_imp_pdp;

  # if not hive_overwrite, set to append mode
  # Note that if the target table (or partition) already has a file whose name collides with any of the filenames contained in filepath, then the existing file will be replaced with the new file.
  if (hive_overwrite.strip().lower() == 'y'):
    hive_overwrite = ' OVERWRITE '
  else:
    hive_overwrite = ' '

  if (csv_delimiter.strip().lower() == 'tab'): csv_delimiter = "\'\\t\'"
  hive_load_query = "load data inpath \'%s\' %s into table %s" % (hdfs_inpath, hive_overwrite, hive_table)
  execute_remote_hive_query( hive_node  = hive_node, hive_port = hive_port, \
                             hive_db    = hive_db, mapred_job_priority = mapred_job_priority, \
                             quiet      = quiet, debug = debug, \
                             hive_query = hive_load_query)
  # verify if table count match csv count
  number_rows = count_hive_table_rows(hive_node  = hive_node, hive_port = hive_port, \
                                      hive_db    = hive_db,   mapred_job_priority = mapred_job_priority, \
                                      quiet      = quiet,     debug = debug, \
                                      hive_table = hive_table)
  num_lines = count_lines(file=csv_file, skip_header=csv_header)
  if ( int(num_lines) == int(number_rows) ):
    legoo.info("file [%s] successfully loaded to hive table [%s]:[%s].[%s]. \n"  %  \
               (csv_file, hive_node, hive_db, hive_table))
  else:
    legoo.error("file [%s] count not match hive table [%s]:[%s].[%s] count. \n"  %  \
                (csv_file, hive_node, hive_db, hive_table))
    raise Exception("[ERROR] file [%s] count not match hive table [%s]:[%s].[%s] count. \n"  %  \
          (csv_file, hive_node, hive_db, hive_table))

def hive_to_mysql( **kwargs ):
  """export [hive table | user defined query ] to csv_file, create mysql table based on csv_file header,
  then load csv_file to mysql table
  --------------------------------------------------------------------------------------------------
  hive_to_mysql(hive_table='fact_imp_pdp', hive_query='select * from bi.fact_imp_pdp limit 1100000')
  --------------------------------------------------------------------------------------------------
  """
  debug                         = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  hive_node                     = kwargs.pop("hive_node", "namenode2s")
  hive_db                       = kwargs.pop("hive_db", "bi")
  hive_table                    = kwargs.pop("hive_table", None)
  csv_file                      = kwargs.pop("csv_file", None)
  hive_query                    = kwargs.pop("hive_query", None)
  mapred_job_priority           = kwargs.pop("mapred_job_priority", "NORMAL")
  mysql_ini                     = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host                    = kwargs.pop("mysql_host", "bidbs")
  mysql_db                      = kwargs.pop("mysql_db", "bi_staging")
  mysql_user                    = kwargs.pop("mysql_user", "root")
  mysql_password                = kwargs.pop("mysql_password", None)
  mysql_table                   = kwargs.pop("mysql_table", None)
  mysql_truncate_table          = kwargs.pop("mysql_truncate_table", "Y")
  csv_delimiter                 = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  csv_optionally_enclosed_by    = kwargs.pop("csv_optionally_enclosed_by", None)
  max_rows                      = kwargs.pop("max_rows", None)
  mysql_create_table            = kwargs.pop("mysql_create_table", "N")
  quiet                         = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # export hive table to csv_file
  csv_file = hive_to_csv(hive_node           = hive_node, \
                         hive_db             = hive_db, \
                         hive_table          = hive_table, \
                         mapred_job_priority = mapred_job_priority, \
                         csv_file            = csv_file, \
                         hive_query          = hive_query, \
                         quiet               = quiet, \
                         debug               = debug)
  # raw_input('press any key to continue ...')
  # dump the first 10 lines to verify
  if (debug.strip().lower() == 'y'):
    csv_dump(csv_file=csv_file, csv_delimiter='tab', lines=10)
  # raw_input('press any key to continue ...')
  # import csv to mysql table
  csv_to_mysql(mysql_host                 = mysql_host, \
               mysql_db                   = mysql_db, \
               mysql_user                 = mysql_user, \
               mysql_password             = mysql_password, \
               mysql_table                = mysql_table, \
               mysql_truncate_table       = mysql_truncate_table, \
               csv_delimiter              = csv_delimiter, \
               csv_optionally_enclosed_by = csv_optionally_enclosed_by, \
               csv_file                   = csv_file, \
               max_rows                   = max_rows, \
               mysql_create_table         = mysql_create_table, \
               quiet                      = quiet, \
               debug                      = debug)
  remove_file(file=csv_file) # remove temp file

def hive_to_csv( **kwargs ):
  """export [hive table | user defined hive_query ] to csv.
  ---------------------------------------------------------------------------------------------------------------
  hive_to_csv(hive_table='fact_imp_pdp')
  hive_to_csv(csv_file='/tmp/dim_listing.csv',hive_query='select * from bi.fact_imp_pdp limit 1100000',debug='Y')
  ---------------------------------------------------------------------------------------------------------------
  """
  debug               = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  hive_node           = kwargs.pop("hive_node", "namenode2s")
  hive_db             = kwargs.pop("hive_db", "bi")
  hive_table          = kwargs.pop("hive_table", None)
  hive_query          = kwargs.pop("hive_query", None)
  mapred_job_priority = kwargs.pop("mapred_job_priority", "NORMAL")
  csv_dir             = kwargs.pop("csv_dir", "/data/tmp/")
  csv_file            = kwargs.pop("csv_file", None)
  quiet               = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))     # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # trulia specific: login to cdh4 cluster namenode2s as dwr
  if (hive_node.strip().lower().split('.')[0] == 'namenode2s'):
    hive_node = 'dwr@' + hive_node

  if (not csv_file):
    if (not hive_table):
      csv_file = csv_dir.strip() + str(os.getpid()).strip() + ".csv"                  # set default csv
    else:
      csv_file = csv_dir.strip() + hive_table + ".csv"                  # set default csv
  else:
    csv_file = csv_dir.strip() + csv_file.strip()                 # set default csv
  if (not hive_query):
    hive_query = "select * from %s.%s" % (hive_db, hive_table) # set default hive_query

  # check and set default value for mapred_job_priority
  if (mapred_job_priority.strip().upper() in ["VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"]):
    mapred_job_priority = mapred_job_priority.strip().upper()
  else:
    legoo.warning("option mapred_job_priority [%s] must in list [VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW]. default to NORMAL." % (mapred_job_priority))
    mapred_job_priority = "NORMAL"

  # hive_query must enclose with quote
  hive_query = '\"use %s; set hive.cli.print.header=true; set mapred.job.priority=%s; ' % (hive_db, mapred_job_priority) + hive_query + ';\"'
  hive_cmd = 'ssh %s hive -e ' % (hive_node) + hive_query + ' > ' + csv_file

  legoo.info("running hive export ...\n[%s]\n" % (hive_cmd))
  with open(csv_file, "w") as outfile:
    rc = subprocess.call(['ssh', hive_node, 'hive', '-e', hive_query], stdout=outfile)

  legoo.info("hive table %s:(%s) exported to %s ..." % (hive_node, hive_query, csv_file))
  return csv_file

def csv_to_mysql(**kwargs):
  """create mysql table in target db (bidbs:bi_staging by default) based on csv header then import
  csv to  mysql table. The other four mysql_host, mysql_db, mysql_truncate_table and debug are optional.
  ------------------------------------------------------------------------------------------------
  csv_to_mysql(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', mysql_create_table = 'Y')
  ------------------------------------------------------------------------------------------------
  """
  debug                        = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the (name, value) pairs in the keyword argument list
  mysql_host                   = kwargs.pop("mysql_host", "bidbs")
  mysql_db                     = kwargs.pop("mysql_db", "bi_staging")
  mysql_user                   = kwargs.pop("mysql_user", None)
  mysql_password               = kwargs.pop("mysql_password", None)
  mysql_create_table           = kwargs.pop("mysql_create_table", "N")
  mysql_table                  = kwargs.pop("mysql_table", None)
  mysql_truncate_table         = kwargs.pop("mysql_truncate_table", "N")
  csv_file                     = kwargs.pop("csv_file", None)
  csv_header                   = kwargs.pop("csv_header", "Y")
  csv_delimiter                = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  csv_optionally_enclosed_by   = kwargs.pop("csv_optionally_enclosed_by", None)
  max_rows                     = kwargs.pop("max_rows", None)
  quiet                        = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))         # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # check number of lines in csv file
  num_lines = count_lines(file=csv_file, skip_header=csv_header)
  if (int(num_lines) == 0):
    legoo.error("%s is empty!" % (csv_file))
    raise TypeError("[ERROR] %s is empty!" % (csv_file))

  # create table if mysql_create_table set to Y
  if (mysql_create_table.strip().lower() == 'y'):
    # create ddl
    (mysql_table_name, ddl) = create_mysql_ddl_from_csv(csv_file           = csv_file, \
                                                        csv_delimiter      = csv_delimiter, \
                                                        table_name         = mysql_table, \
                                                        max_rows           = max_rows, \
                                                        mysql_create_table = mysql_create_table, \
                                                        quiet              = quiet, \
                                                        debug              = debug)
    # create table
    execute_mysql_query(mysql_host     = mysql_host, \
                        mysql_db       = mysql_db, \
                        mysql_user     = mysql_user, \
                        mysql_password = mysql_password, \
                        mysql_query    = ddl, \
                        quiet          = quiet, \
                        debug          = debug)

  # set mysql_table to mysql_table_name if not specified
  if (not mysql_table):
    mysql_table = mysql_table_name

  if (mysql_truncate_table.strip().lower() == 'y'):
    execute_mysql_query(mysql_host=mysql_host, mysql_db=mysql_db, \
                        mysql_user=mysql_user, mysql_password=mysql_password, \
                        mysql_query="TRUNCATE TABLE %s.%s" % (mysql_db, mysql_table), \
                        quiet = quiet, debug=debug)

  # check table row count
  mysql_query = "select count(*) from %s.%s;" % (mysql_db, mysql_table)

  (affected_rows, number_rows) = execute_mysql_query(mysql_host=mysql_host, mysql_db=mysql_db, \
                                                     mysql_user=mysql_user, mysql_password=mysql_password, \
                                                     mysql_query=mysql_query, row_count='Y', \
                                                     quiet = quiet, debug=debug)
  table_count_before_load = number_rows

  # load csv into mysql table
  csv_to_mysql_table(mysql_host=mysql_host, mysql_db=mysql_db, mysql_user=mysql_user, \
                     mysql_password=mysql_password, mysql_table=mysql_table, \
                     csv_file=csv_file, csv_header=csv_header, csv_delimiter=csv_delimiter, \
                     csv_optionally_enclosed_by=csv_optionally_enclosed_by, \
                     quiet = quiet, debug=debug)


  (affected_rows, number_rows) = execute_mysql_query(mysql_host=mysql_host, mysql_db=mysql_db, \
                                                     mysql_user=mysql_user, mysql_password=mysql_password, \
                                                     mysql_query=mysql_query, row_count='Y', \
                                                     quiet = quiet, debug=debug)
  table_count_after_load = number_rows
  # delta: diff between table count before load and after load
  number_rows = int(table_count_after_load) - int(table_count_before_load)
  legoo.info("MySQL table [%s]:[%s].[%s] load count ==>> [%s]"  %  (mysql_host, mysql_db, mysql_table, number_rows))

  # verify the csv line count and table count
  if ( int(num_lines) == int(number_rows) ):
    legoo.info("file [%s] successfully loaded to mysql table [%s]:[%s].[%s]"  %  (csv_file, mysql_host, mysql_db, mysql_table))
  else:
    legoo.error("file [%s] count does not match mysql table [%s]:[%s].[%s] load count"  %  (csv_file, mysql_host, mysql_db, mysql_table))
    raise Exception("[ERROR] file [%s] count does not match mysql table [%s]:[%s].[%s] load count"  %  (csv_file, mysql_host, mysql_db, mysql_table))

def csv_to_mysql_table(**kwargs):
  """import csv to existing mysql table in target db (bidbs:bi_staging by default)  with  5 parameters.
  mysql_table is required. The other four mysql_host, mysql_db, mysql_truncate_table  and debug are optional.
  -----------------------------------------------------------------------------------------------------
  csv_to_mysql_table(mysql_table='tmp_table', csv_file='/tmp/hive_bi_dim_listing.csv', csv_delimiter='tab')
  -----------------------------------------------------------------------------------------------------
  """
  debug                      = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_host                 = kwargs.pop("mysql_host", "bidbs")
  mysql_db                   = kwargs.pop("mysql_db", "bi_staging")
  mysql_user                 = kwargs.pop("mysql_user", None)
  mysql_password             = kwargs.pop("mysql_password", None)
  mysql_table                = kwargs.pop("mysql_table", None)
  csv_file                   = kwargs.pop("csv_file", None)
  csv_delimiter              = kwargs.pop("csv_delimiter", 'tab')            # default to tab csv_delimiter
  csv_header                 = kwargs.pop("csv_header", "Y")
  csv_optionally_enclosed_by = kwargs.pop("csv_optionally_enclosed_by", None)
  quiet                      = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  # run mysql dml
  if (not mysql_table):
    legoo.error("need to specify mysql_table")
    raise TypeError("need to specify mysql_table")

  # add quote to csv_delimiter
  if (csv_delimiter.strip().lower() == 'tab'):
    csv_delimiter = '\\t'
  # print '%s:csv_delimiter =>>>> [%s]' %(sys._getframe().f_code.co_name, csv_delimiter)

  if (csv_optionally_enclosed_by):
    enclosed_by = "OPTIONALLY ENCLOSED BY '%s'" % (csv_optionally_enclosed_by)
  else:
    enclosed_by = ''

  if (csv_header.strip().lower() == 'n'):
    ignore_line = ''
  else:
    ignore_line = 'IGNORE 1 LINES'

  # if (csv_optionally_enclosed_by = '\"')
  mysql_dml = """LOAD DATA LOCAL INFILE '%s'
  INTO TABLE %s
  FIELDS TERMINATED BY '%s'  %s  %s""" % (csv_file, mysql_table, csv_delimiter, enclosed_by, ignore_line)

  # adhocdb/adhocmaildb cant LOAD DATA using MySQLDB client. it is possible due to older version of  MySQL Server
  # fall back to less preferred system command
  if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db
    mysql_cmd = 'mysql -h%s -u%s -p%s %s -e "%s"' % \
                ( mysql_host, mysql_user, mysql_password, mysql_db, mysql_dml)
    legoo.info("running MySQL command: [%s]" % (mysql_cmd))
    os.system( mysql_cmd )
  else:
    execute_mysql_query(mysql_host=mysql_host, mysql_db=mysql_db, \
                        mysql_user=mysql_user, mysql_password=mysql_password, \
                        mysql_query=mysql_dml, \
                        quiet = quiet, debug=debug)

def execute_remote_hive_query(**kwargs):
  """execute hive query on remote hive node
  -------------------------------------------------------
  execute_remote_hive_query(hive_query='desc top50_ip;')
  -------------------------------------------------------
  """
  debug     = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  hive_node           = kwargs.pop("hive_node", "namenode2s")
  hive_port           = kwargs.pop("hive_port", 10000)
  hive_db             = kwargs.pop("hive_db", "staging")
  hive_query          = kwargs.pop("hive_query", None)
  mapred_job_priority = kwargs.pop("mapred_job_priority", "NORMAL")
  quiet               = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit
  # check and set default value for mapred_job_priority
  if (mapred_job_priority.strip().upper() in ["VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"]):
    mapred_job_priority = mapred_job_priority.strip().upper()
  else:
    legoo.warning("option mapred_job_priority [%s] must in list [VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW]. default to NORMAL." % (mapred_job_priority))
    mapred_job_priority = "NORMAL"

  from hive_service import ThriftHive
  from hive_service.ttypes import HiveServerException
  from thrift import Thrift
  from thrift.transport import TSocket
  from thrift.transport import TTransport
  from thrift.protocol import TBinaryProtocol

  # hive_query must enclose with quote
  hive_query_with_quote = '\"use %s; %s\"' % (hive_db, hive_query)
  # print hive_query_with_quote
  hive_cmd = 'ssh %s hive -e %s' % (hive_node, hive_query_with_quote)
  legoo.info("running hive query on [%s]:[%s] ==>> [%s]" % (hive_node, hive_db, hive_query))
  # rc = subprocess.call(['ssh', hive_node, 'hive', '-e', hive_query_with_quote])
  result_set = [0]

  try:
    transport = TSocket.TSocket(hive_node, hive_port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHive.Client(protocol)
    transport.open()
    client.execute("use %s" % (hive_db))
    client.execute("set mapred.job.priority=%s" % (mapred_job_priority))
    client.execute(hive_query)
    # client.execute("desc dim_listing")
    # client.execute("select * from dim_listing limit 10")
    result_set = client.fetchAll()
    transport.close()
    return result_set
  except Thrift.TException, tx:
    raise Exception('[ERROR] %s' % (tx.message))

def execute_mysql_query(**kwargs):
  """return tuple (rows_affected, number_of_rows) after execute mysql query on target db (bidbs:bi_staging by default).
  -------------------------------------------------------------------------------------------------
  execute_mysql_query(mysql_host='bidbs', mysql_db='bi_staging', mysql_query='select current_date')
  -------------------------------------------------------------------------------------------------
  """
  debug           = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_ini       = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host      = kwargs.pop("mysql_host", "bidbs")
  mysql_db        = kwargs.pop("mysql_db", "bi_staging")
  mysql_user      = kwargs.pop("mysql_user", None)
  mysql_password  = kwargs.pop("mysql_password", None)
  mysql_query     = kwargs.pop("mysql_query", None)
  row_count       = kwargs.pop("row_count", "N")
  quiet           = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit
  number_of_rows = rows_affected = 0 # set default value
  legoo.info("running mysql query on [%s]:[%s] ==>> [%s]" % (mysql_host, mysql_db, mysql_query))
  try:
    mysql_conn = create_mysql_connection(mysql_ini      = mysql_ini, \
                                         mysql_host     = mysql_host, \
                                         mysql_db       = mysql_db, \
                                         mysql_user     = mysql_user, \
                                         mysql_password = mysql_password, \
                                         quiet          = quiet, \
                                         debug          = debug \
                                         )
    cursor = mysql_conn.cursor()
    rows_affected = cursor.execute(mysql_query)
    if (row_count.strip().lower() == 'y'):
      (number_of_rows,)=cursor.fetchone() # used for counts
    else:
      rs = cursor.fetchall()
      if (len(rs) > 0):
        pprint(rs)

    if (debug.strip().lower() == 'y'):
      legoo.info('[%s] rows affected by query [%s].' % (rows_affected, mysql_query))
      legoo.info('[INFO] [%s] number of rows returned by query [%s].' % (number_of_rows, mysql_query))
    return  (rows_affected, number_of_rows)
  except MySQLdb.Error as e:
    legoo.info('[ERROR] [%s] failed on [%s].[%s]' % ( mysql_query, mysql_host, mysql_db))
    legoo.info("[ERROR] %d: %s" % (e.args[0], e.args[1]))
    raise
  finally:
    cursor.close()
    mysql_conn.close()

def qa_mysql_table(**kwargs):
  debug           = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_ini           = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host          = kwargs.pop("mysql_host", "bidbs")
  mysql_db            = kwargs.pop("mysql_db", "bi_staging")
  mysql_user          = kwargs.pop("mysql_user", None)
  mysql_password      = kwargs.pop("mysql_password", None)
  mysql_query         = kwargs.pop("mysql_query", None)
  comparison_operator = kwargs.pop("comparison_operator", None)
  threshhold_value    = kwargs.pop("threshhold_value", None)
  quiet               = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  (affected_rows, number_rows) = execute_mysql_query(mysql_host          = mysql_host, \
                                                     mysql_db            = mysql_db, \
                                                     mysql_user          = mysql_user, \
                                                     mysql_password      = mysql_password, \
                                                     mysql_query         = mysql_query, \
                                                     row_count           = 'Y', \
                                                     quiet               = quiet, \
                                                     debug               = debug)

  # print "number_rows => [%s]; options.comparison_operator => [%s];  options.threshhold => [%s]" % \        (number_rows,  options.comparison_operator,  options.threshhold)

  if ( (not mysql_query) or (not comparison_operator) or (not threshhold_value)):
    legoo.error("option mysql_query, comparison_operator, threshhold_value not all set. must specify all three ... ")      # log error
    raise TypeError("option mysql_query, comparison_operator, threshhold_value not all set. must specify all three...")   # raise error and exit

  # build operator dictionary to python built in operator
  ops = {"=":  operator.eq,
         "==": operator.eq,
         "!=": operator.ne,
         "<>": operator.ne,
         "<":  operator.lt,
         "<=": operator.le,
         ">":  operator.gt,
         ">=": operator.ge
         }
  # may the key to the build-in operator
  op_func = ops[comparison_operator]

  if op_func(int(number_rows), int(threshhold_value)):
    legoo.info('[INFO] [%s] passed test: {[%s] [%s] [%s]}' % (mysql_query, number_rows, comparison_operator, threshhold_value))
  else:
    legoo.error('[ERROR] [%s] failed test: {[%s] [%s] [%s]}' % (mysql_query, number_rows, comparison_operator, threshhold_value))
    raise TypeError('[ERROR] [%s] failed test: {[%s] [%s] [%s]}' % (mysql_query, number_rows, comparison_operator, threshhold_value))

def create_mysql_connection(**kwargs):
  """return myql connection object based on configurations in mysql_ini. For security reason,
  user/password pulled from mysql.ini. extend ini file if necessary.
  -------------------------------------------------------------------------------------------
  create_mysql_connection(mysql_host='bidbs', mysql_db='bi_staging', debug='N')
  -------------------------------------------------------------------------------------------
  """
  debug     = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_ini       = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host      = kwargs.pop("mysql_host", "bidbs")
  mysql_db        = kwargs.pop("mysql_db", "bi_staging")
  mysql_user      = kwargs.pop("mysql_user", None)
  mysql_password  = kwargs.pop("mysql_password", None)
  quiet           = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit
  # parse the ini file to pull db variables
  config = ConfigParser.ConfigParser()
  # find mysql_ini in the same directory as script
  config.read(os.path.join(os.path.dirname(__file__), mysql_ini))

  # extend e mysql.ini if necessary
  # set default mysql_user from mysql_ini
  if (not mysql_user):
    if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db.
      mysql_user = config.get(mysql_host, "user")
    else:
      mysql_user = config.get('default', "user")

  # set default mysql_password from mysql_ini
  if (not mysql_password):
    if (mysql_host not in trulia_mysql_host): # adhocdb use a non-standard password and db.
      mysql_password = config.get(mysql_host, "password")
    else:
      mysql_password = config.get('default', "password")
  mysql_conn = MySQLdb.connect(host = mysql_host, # the host
                            user = mysql_user, # user name
                            passwd = mysql_password, # password
                            db = mysql_db) # default database/schema
  # test connection and print out debug info
  if (debug.strip().lower() == 'y'):
    cursor = mysql_conn.cursor()
    cursor.execute("SELECT CURRENT_DATE()")
    data = cursor.fetchone()    # fetch a single row using fetchone() method.
    legoo.info("FUNCTION STARTS: [ %s ] >>>>>>" % (sys._getframe().f_code.co_name))
    legoo.info("script                => %s" % (os.path.abspath(__file__)))
    legoo.info("mysql_ini             => %s/%s" %(os.getcwd(), mysql_ini))
    legoo.info("host:db               => %s:%s" % (host, mysql_db))
    legoo.info("SELECT CURRENT_DATE() => %s" % (data))
    legoo.info("FUNCTION ENDS  <<<<<<\n")
    cursor.close()
  return mysql_conn

def create_mysql_ddl_from_csv(**kwargs):
  """return table name, mysql table ddl based on csv header. by default, scan the whole file to detect column length.
  """
  debug              = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  csv_file           = kwargs.pop("csv_file", None)
  csv_delimiter      = kwargs.pop("csv_delimiter", 'tab') # default to tab
  table_name         = kwargs.pop("table_name", None)
  max_rows           = kwargs.pop("max_rows", None)
  mysql_create_table = kwargs.pop("mysql_create_table", "N")
  quiet              = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit
  # initilize variables
  csv.field_size_limit(sys.maxsize)        # override default size 131072
  column_len = {}                          # dictionary for column and length
  i = 0                                    # row counter
  ddl = ''                                 # initilize ddl to empty string
  if (csv_delimiter.strip().lower() == 'tab'): csv_delimiter = '\t'

  # set default table name from file name
  if not table_name:
    # get the base file name then split to filename and extension
    (filename, extension) =  os.path.splitext(os.path.basename(csv_file))
    table_name = filename   # build table table with csv prefix
    table_name = table_name.replace('.', '_')  # replace . with _ in table name

  # scan csv file up to max_rows to find the max column length
  with open(csv_file, "rb") as csv_file_new:
    # create reader object
    reader = csv.DictReader(csv_file_new, delimiter=csv_delimiter)
    # initialize column length dictionary for ddl
    for fn in reader.fieldnames:
      column_len[fn] = 0

    for row in reader:
      i += 1
       # for large csv files, print out progress indicator for every 100K rows
      if (i % 100000) == 0: print '[INFO] [%s] scanning %d rows to calculate the max column length ...' % (csv_file, i)
      # find the max field length
      for fn in reader.fieldnames:
        # swap with current column_len if greater
        if len(row[fn]) > column_len[fn]: column_len[fn] = len(row[fn])
        # for large file, stop after reaching max_rows
      if (max_rows and int(i) == int(max_rows)):
        # print "here $$$", i, '==>>',  max_rows
        for fn in column_len:
          column_len[fn] *= 2  # estimate by doubling the current max column length
        break

  # set max_column_name_len for ddl formating, add 8 to seperate column name and data type
  max_column_name_len = len(max(column_len.keys(), key=len)) + 8

  # ddl with table name and primary key id
  ddl = ''
  ddl += "CREATE TABLE %s (\n" % (table_name)
  # create ddl from reader.fieldnames which preserve the original csv order
  for index, fn in enumerate(reader.fieldnames):
    if (column_len[fn] > 2000):
      data_type = 'TEXT'
    else:
      data_type = 'VARCHAR'

    if (index + 1) < len(reader.fieldnames):
      ddl += fn.ljust(max_column_name_len) + data_type + '(' + str(column_len[fn]) + '), \n'
    else: # last column
      ddl += fn.ljust(max_column_name_len) + data_type + '(' + str(column_len[fn]) + ') \n);'
  if (debug.strip().upper() == 'Y'):
    legoo.info('[INFO] [%s] => scanned %d rows to calculate the max column length. \n' % (csv_file, i))
    legoo.info(''.ljust(50, '='), '\n',  ddl, '\n', ''.ljust(50, '='))
  return (table_name, ddl)

def create_hive_ddl_from_csv(**kwargs):
  """return table name, table ddl based on csv header
  ------------------------------------------------------------------------------------------------------------------
  (table_name, hive_ddl)=create_hive_ddl_from_csv(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab')
  ------------------------------------------------------------------------------------------------------------------
  """
  debug         = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # pretty print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  csv_file      = kwargs.pop("csv_file", None)
  csv_delimiter = kwargs.pop("csv_delimiter", 'tab') # default to tab
  table_name    = kwargs.pop("table_name", None)
  quiet         = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  # set default arguments
  if (not table_name): # set default table name from file name
    # get the base file name then split to filename and extension
    (filename, extension) =  os.path.splitext(os.path.basename(csv_file))
    table_name = filename   # build table table with csv prefix
  # convert csv_delimiter to octal code for hive
  if (csv_delimiter.strip().lower() == 'tab'):
    csv_delimiter = '\t'
    hive_csv_delimiter = '\\011'
  elif (csv_delimiter.strip() == ','):
    hive_csv_delimiter = '\\054'
  # initialize variables
  hive_keywords = ['location', 'date', 'format']

  # scan csv file up to max_rows to find the max column length
  with open(csv_file, "rb") as csv_file_new:
    # create reader object
    reader = csv.DictReader(csv_file_new, delimiter=csv_delimiter)

    # find the max length of column names for nice formatting
    max_fn_len = len(max(list(reader.fieldnames), key=len)) + 4

    hive_ddl = "CREATE TABLE %s (\n" % (table_name)
    # create hive_ddl from reader.fieldnames which preserve the original csv order
    for index, fn in enumerate(reader.fieldnames):
      data_type = 'string' # set default data type
      # if column name from csv header is hive keywords, postfix with _new
      if (fn.lower().strip() in hive_keywords):
        fn = fn.lower().strip() + '_new'
      if (index + 1) < len(reader.fieldnames):
        hive_ddl += fn.ljust(max_fn_len) + data_type + ', \n'
      else: # last column
        hive_ddl += fn.ljust(max_fn_len) + data_type + '\n)\n'
    # add csv_delimiter specfication
    hive_ddl += "ROW FORMAT DELIMITED FIELDS TERMINATED BY \'%s\'\nSTORED AS TEXTFILE" % (hive_csv_delimiter)

  csv_file_new.close()
  return (table_name, hive_ddl)

def csv_dump(**kwargs):
  """dump first n rows from csv
  --------------------------------------------------------------------
  csv_dump(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', lines=5)
  --------------------------------------------------------------------
  """
  # print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  csv_file      = kwargs.pop("csv_file", None)
  csv_delimiter = kwargs.pop("csv_delimiter", 'tab') # default to tab
  lines         = kwargs.pop("lines", 10) # default to tab
  line_number   = kwargs.pop("line_number", 2) # default to line number 2, first line is header
  debug         = kwargs.pop("debug", "N")
  if kwargs:
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))

  # initilize variables
  csv.field_size_limit(sys.maxsize)        # override default size 131072
  column_len = {}                          # dictionary for column and length
  i = 0                                    # row counter
  if (csv_delimiter.strip().lower() == 'tab'): csv_delimiter = '\t'

  # if need to get lines from the middle of file
  # create a new temp file start with line number
  if (line_number > 2):
    tmp_file = '/tmp/temp_csv.csv'
    tmp_csv_cmd = "head -1 %s > %s; " % (csv_file, tmp_file)
    tmp_csv_cmd = tmp_csv_cmd + "tail -n +%s  %s | head -%s >> %s;" % (line_number, csv_file, lines, tmp_file)
    print tmp_csv_cmd
    os.system( tmp_csv_cmd )
    csv_file = tmp_file

  # scan csv file up to max_rows to find the max column length
  with open(csv_file, "rb") as csv_file_new:
    # create reader object
    reader = csv.DictReader(csv_file_new, delimiter=csv_delimiter)

    # find the max field name lenght for formating
    max_fn_len = len(max(list(reader.fieldnames), key=len))

    for row in reader:
      # print "\nRECORD NUMBER %d: " % (i)
      print '='.ljust(max_fn_len + 29, '=')
      print "Line number".ljust( max_fn_len + 9), "\t<<<<\t[%s]" % (int(line_number) + i)
      column_index = 1 # track column index
      for fn in reader.fieldnames:
        print "[c%s] " % (str(column_index).rjust(3, '0')), fn.ljust( max_fn_len), "\t==>>\t[%s]" % (row[fn])
        column_index += 1
      # print '='.ljust(max_fn_len + 20, '=')
      i += 1
      if (int(i) == min(int(lines), 100)):
        break

def wait_for_table(**kwargs):
  """check if table exists, and updated after [mysql_table_update_after]
  if not, retry based on [sleep_interval], [num_retry] and/or [stop_at]
  NOTE:
  1. need access to INFORMATION_SCHEMA.TABLES to retrieve update_time
  2. option [STOP_AT] i.e. [2013-10-08 15:30], together with [mysql_table_update_after]
     i.e. [2013-10-09 14:25], define the table wait window
  3. option [ETL_TABLE] and [ETL_JOB] are trulia specific which retrive table last update
     from proprietary [AUDIT_JOB] database
  """
  TABLE_FOUND = False # init variable to False
  i = 0              # init counter

  debug                        = kwargs.pop("debug", "N")
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)   # print kwargs
  # dictionary initialized with the name=value pairs in the keyword argument list
  mysql_ini                    = kwargs.pop("mysql_ini", "mysql.ini")
  mysql_host                   = kwargs.pop("mysql_host", None)
  mysql_db                     = kwargs.pop("mysql_db", None)
  mysql_user                   = kwargs.pop("mysql_user", None)
  mysql_password               = kwargs.pop("mysql_password", None)
  mysql_table                  = kwargs.pop("mysql_table", None)
  etl_table                    = kwargs.pop("etl_table", None)
  etl_job                      = kwargs.pop("etl_job", None)
  mysql_table_update_after     = kwargs.pop("mysql_table_update_after", None)
  sleep_interval               = kwargs.pop("sleep_interval", 60)
  num_retry                    = kwargs.pop("num_retry", None)
  stop_at                      = kwargs.pop("stop_at", None)   # [hh:mm] i.e. "14:30"
  quiet                        = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)   # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  if ( not mysql_table and not etl_table and not etl_job ):
    legoo.error("Must specify one of options of [MYSQL_TABLE], [ETL_TABLE], and [ETL_JOB] ... ")      # log error
    raise TypeError("Must specify one of options of [MYSQL_TABLE], [ETL_TABLE], and [ETL_JOB] ... ")  # raise error and exit

  if ( num_retry ):
    num_retry          = int(str(num_retry).strip().lower()) # format and convert to int
    if ( num_retry < 1) :
      legoo.error("invalid value [%s] for [num_retry]. try again with positive integer ... " % (num_retry)) # log error
      raise ValueError("invalid value [%s] for [num_retry]. try again with positive integer ... " \
                       % (num_retry)) # raise error and exit
  else:
    num_retry = 0  # default to 0 if not specified

  sleep_interval     = int(str(sleep_interval).strip().lower()) # format and convert to int
  if ( sleep_interval < 1) :
    legoo.error("invalid value [%s] for [sleep_interval]. try again with positive integer ... " \
                % (sleep_interval))      # log error
    raise ValueError("invalid value [%s] for [sleep_interval]. try again with positive integer ... " \
                     % (sleep_interval)) # raise error and exit

  if ( (not stop_at) and (not num_retry)):
    legoo.error("option stop_at and num_retry not set. must specify one ... ")      # log error
    raise TypeError("option stop_at and num_retry not set. must specify one ...")   # raise error and exit

  # create datetime object stop_at i.e. "14:30" if specified
  if ( stop_at ):
    stop_at_dt = datetime.datetime.strptime(stop_at, '%Y-%m-%d %H:%M')
  else:
    stop_at_dt = datetime.datetime.now()

  if ( mysql_table_update_after ):
    mysql_table_update_after_dt = datetime.datetime.strptime(mysql_table_update_after, '%Y-%m-%d %H:%M')
  else:
    mysql_table_update_after_dt = datetime.datetime.fromtimestamp(0).strftime("%Y-%m-%d %H:%M") # set default

  if ( num_retry ):
    num_retry          = int(str(num_retry).strip().lower()) # format and convert to int
    if ( num_retry < 1) :
      legoo.error("invalid value [%s] for [num_retry]. try again with positive integer ... " % (num_retry)) # log error
      raise ValueError("invalid value [%s] for [num_retry]. try again with positive integer ... " \
                       % (num_retry)) # raise error and exit
  else:
    num_retry = 0  # default to 0 if not specified

  # build query depending options of [MYSQL_TABLE], [ETL_TABLE], and [ETL_JOB]
  if (mysql_table):
    mysql_query = """SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                     WHERE table_name = '%s'
                     AND update_time >= '%s'
                     """ % ( mysql_table, mysql_table_update_after_dt)
  elif (etl_table):
    mysql_query = """SELECT COUNT(*)
                     FROM audit.audit_job_detail ajd, audit.audit_job aj
                     WHERE ajd.job_id = aj.job_id
                     AND LOWER(Task_Target_Table) = '%s'
                     AND Task_End_Time > '%s'
                     AND LOWER(job_success_flag) = 'y'
                     AND LOWER(job_qa_success_flag) = 'y'
                     """ % ( etl_table, mysql_table_update_after_dt)
  elif (etl_job):
    mysql_query = """SELECT COUNT(*)
                     FROM audit.audit_job
                     WHERE LOWER(job_name) LIKE '%s'
                     AND LOWER(job_success_flag) = 'y'
                     AND job_end_time >= '%s'
                     """ % ( etl_job, mysql_table_update_after_dt)

  # variable for logging
  target = ''.join(filter(None, (mysql_table, etl_table,  etl_job)))
  # greedy algorithm: when stop_at and num_retry both set, progam continues either stop_at or num_retry satisfies
  while ( (datetime.datetime.now() < stop_at_dt) or (i < num_retry) ):
    (affected_rows, number_rows) = execute_mysql_query(mysql_ini=mysql_ini, mysql_host=mysql_host, mysql_db=mysql_db, \
                                                       mysql_user=mysql_user, mysql_password=mysql_password, \
                                                       mysql_query=mysql_query, row_count='Y', debug=debug)
    if (int(number_rows) > 0):
      TABLE_FOUND = True # set flag to true if table exists
      legoo.info("table [%s] is ready" % (target))
      break
    else:
      legoo.info("check #[%d] if table [%s] is ready" % (i, target))
      sleep(sleep_interval)
    i += 1
    if (debug.strip().lower() == 'y'):
      print i, "=>>",  num_retry, '    ',  datetime.datetime.now(),  "=>>", stop_at_dt

  if (not TABLE_FOUND):
    legoo.error("table [%s] is NOT ready" % (target))           # log error
    raise TypeError("table [%s] is NOT ready" % (target))       # raise error and exit

def wait_for_file(**kwargs):
  """check if file exists or newer, otherwise, wait and retry
  ---------------------------------------------------------
  wait_for_file(num_retry = 10, sleep_interval=60, file = 'test')
  ---------------------------------------------------------
  """
  debug              = kwargs.pop("debug", "N")
  # print kwargs
  if (debug.strip().lower() == 'y'):
    pprint(kwargs)
  stop_at            = kwargs.pop("stop_at", None)  # [yyyy-mm-dd mmhh:mm] i.e. "2013-10-08 14:30"
  mtime_after        = kwargs.pop("mtime_after", None)    # [yyyy-mm-dd mmhh:mm] i.e. "2013-10-08 14:30"
  # dictionary initialized with the name=value pairs in the keyword argument list
  num_retry          = kwargs.pop("num_retry", None)
  sleep_interval     = kwargs.pop("sleep_interval", 60)
  sleep_interval     = int(str(sleep_interval).strip().lower()) # format and convert to int
  if ( sleep_interval < 1) :
    legoo.error("invalid value [%s] for [sleep_interval]. try again with positive integer ... " \
                % (sleep_interval))      # log error
    raise ValueError("invalid value [%s] for [sleep_interval]. try again with positive integer ... " \
                     % (sleep_interval)) # raise error and exit
  file               = kwargs.pop("file", None)
  if ( not file ):
    legoo.error("option file not set. must specify file ... ")      # log error
    raise TypeError("option file not set. must specify file ... ")   # raise error and exit

  quiet              = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  FILE_FOUND = False # init variable to False
  i = 0              # init counter

  if ( (not stop_at) and (not num_retry)):
    legoo.error("option stop_at and num_retry not set. must specify one ... ")      # log error
    raise TypeError("option stop_at and num_retry not set. must specify one ...")   # raise error and exit

  if ( stop_at ):
    stop_at_dt = datetime.datetime.strptime(stop_at, '%Y-%m-%d %H:%M')
  else:
    stop_at_dt = datetime.datetime.now()

  if ( mtime_after ):
    mtime_after_dt = datetime.datetime.strptime(mtime_after, '%Y-%m-%d %H:%M')
  else:
    mtime_after_dt = datetime.datetime.fromtimestamp(0) # set default

  if ( num_retry ):
    num_retry          = int(str(num_retry).strip().lower()) # format and convert to int
    if ( num_retry < 1) :
      legoo.error("invalid value [%s] for [num_retry]. try again with positive integer ... " % (num_retry)) # log error
      raise ValueError("invalid value [%s] for [num_retry]. try again with positive integer ... " \
                       % (num_retry)) # raise error and exit
  else:
    num_retry = 0  # default to 0 if not specified

  # greedy algorithm: when stop_at and num_retry both set, progam continues either stop_at or num_retry satisfies
  while ( (datetime.datetime.now() < stop_at_dt) or (i < num_retry) ):
    if (os.path.isfile( file )):
      # check if file stopped being written
      prv_size = os.path.getsize( file )
      sleep(10) # wait 30 seconds then check the file size again
      post_size = os.path.getsize( file )

      # get file ctime
      modify_dt    = datetime.datetime.fromtimestamp(os.path.getmtime(file))
      # check file 1) stop grower 2) modified after mtime_after
      if ( int(prv_size) == int(post_size) and (modify_dt >= mtime_after_dt)):
        FILE_FOUND = True # set flag to true if file exists
        legoo.info("file [%s] found" % (file))
        break
      elif (int(prv_size) <> int(post_size)):
        legoo.info("check #[%d] file [%s] exits but still being written. try again in [%s] seconds..." \
                   % (i, file, sleep_interval))
      elif (modify_dt < mtime_after_dt):
        legoo.info("check #[%d] file [%s] exits but modify_time [%s] before mtime_after [%s]. try again in [%s] seconds..." \
                   % (i, file,  modify_dt, mtime_after_dt, sleep_interval))

    else:
      legoo.info("check #[%d] if file [%s] exits" % (i,  file))
      sleep(sleep_interval)
    i += 1

  if (not FILE_FOUND):
    legoo.error("file [%s] not exits" % (file))           # log error
    raise TypeError("file [%s] not exits" % (file))       # raise error and exit

def dos_to_unix( orig_file, new_file=None):
  """call shell utility dos_to_unix to convert file format from dos to unix
  -------------------------------------------------------------------------
  dos_to_unix('/tmp/msa.csv')
  dos_to_unix(orig_file='/tmp/msa.csv', new_file='/tmp/msa2.csv')
  -------------------------------------------------------------------------
  """
  if not new_file: new_file = file + "_new"
  rc = subprocess.call(["dos_to_unix", "-n", file, new_file])
  return new_file


def send_mail(**kwargs):
  """send email
  ---------------------------------------------------------
  ---------------------------------------------------------
  """
  debug              = kwargs.pop("debug", "N")
  # print kwargs
  if (debug.strip().lower() == 'y'):
      pprint(kwargs)
  # dictionary initialized with the name=value pairs in the keyword argument list
  sender             = kwargs.pop("sender", None)

  receivers          = kwargs.pop("receivers", []) # ['pluo@trulia.com']
  if not receivers:
      legoo.error("option receivers is missing")
      raise TypeError("option receivers is missing")
  elif type(receivers) <> list:
      # convert comma seperate string to list.
      receivers = [r.strip() for r in receivers.split(',') ]

  subject            = kwargs.pop("subject", None)
  if not subject:
      legoo.warning("subject is NULL")
  smtp_server        = kwargs.pop("smtp_server", "mx1.sv2.trulia.com")
  smtp_port        = kwargs.pop("smtp_port", 25)
  body_text          = kwargs.pop("body_text", None)
  body_text_file     = kwargs.pop("body_text_file", None) # ['foo.py', '../show_env.py']
  body_html          = kwargs.pop("body_html", None)
  body_html_file     = kwargs.pop("body_html_file", None)
  attachment_files   = kwargs.pop("attachment_files", None)
  attachment_dir     = kwargs.pop("attachment_dir", None)

  quiet              = kwargs.pop("quiet", "N")
  if (quiet.strip().lower() == 'y'):
    legoo.removeHandler(info_hand)     # suppress logging if variable quiet set to Y
  if kwargs:
    legoo.error("Unsupported configuration options %s" % list(kwargs))                   # log error
    raise TypeError("[ERROR] Unsupported configuration options %s" % list(kwargs))       # raise error and exit

  # import smtp and related modules
  import smtplib, os
  from email.MIMEMultipart import MIMEMultipart
  from email.MIMEBase import MIMEBase
  from email.MIMEText import MIMEText
  from email.Utils import COMMASPACE, formatdate
  from email import Encoders
  from os import listdir
  from os.path import isfile, join

  # build message
  msg = MIMEMultipart()
  msg['From'] = sender
  msg['To'] = COMMASPACE.join(receivers)
  msg['Date'] = formatdate(localtime=True)
  msg['Subject'] = subject

  # build the plain text body
  if body_text:
      msg.attach(MIMEText(body_text, 'plain'))
  if body_text_file:
      msg.attach(MIMEText(open(body_text_file,"rb").read(), 'plain'))

  # build html body
  if body_html:
      msg.attach(MIMEText(body_html, 'html'))
  if body_html_file:
      msg.attach(MIMEText(open(body_html_file,"rb").read(), 'html'))

  # warning: if body is EMPTY
  if not ( body_html or body_html_file or body_text or body_text_file):
      legoo.warning("message body if empty, specify one of boby options [body_html, body_html_file, body_text, body_text_file]")

  # convert comma seperate string to list
  if attachment_files and type(attachment_files) <> list:
      attachment_files = attachment_files.split(',')

  # append list of files from attachment_dir to attachment_files:
  if attachment_dir:
      attachment_files += [ f for f in listdir(attachment_dir) if isfile(join(attachment_dir,f)) ]

  # attach files
  if attachment_files:
      for f in attachment_files:
          part = MIMEBase('application', "octet-stream")
          part.set_payload( open(f.strip(),"rb").read() )
          Encoders.encode_base64(part)
          part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
          msg.attach(part)

  # send mail
  smtp = smtplib.SMTP(smtp_server, smtp_port)
  smtp.sendmail(sender, receivers, msg.as_string())
  smtp.close()

def main():
  # create_mysql_ddl_from_csv(dos_to_unix('/tmp/msa.csv'), ',')
  # DEBUG='N'
  # (table_name, mysql_ddl) = create_mysql_ddl_from_csv(csv_file="/tmp/fact_imp_pdp.csv", csv_delimiter = 'tab', mysql_create_table = 'Y', debug='Y')
  # (table_name, mysql_ddl) = create_mysql_ddl_from_csv(csv_file="/tmp/dim_listing_delta.csv", csv_delimiter = 'tab', mysql_create_table = 'Y', max_rows=60000)
  # csv_to_mysql(csv_file="/tmp/dim_listing_delta.csv", csv_delimiter='tab', mysql_create_table='Y')
  # execute_mysql_query(mysql_host='bidbs', mysql_db='bi', mysql_query='select count(*) from dim_property2', row_count='Y')
  # hive_to_csv(csv_file='dim_listing.csv',hive_node='namenode2s', hive_query='select * from bi.dim_listing limit 1000000', mapred_job_priority = 'HIGH', debug='Y')
  # hive_to_mysql(hive_table='fact_imp_pdp', hive_query='select * from bi.fact_imp_pdp limit 2000000', create_table = 'Y')
  # create_mysql_connection(mysql_host='bidbs', mysql_db='bi_staging', debug='Y')
  # (affected_rows, number_rows) = execute_mysql_query(mysql_host='bidbs', mysql_db='bi', mysql_query='select count(*) from dim_property', row_count='Y')
  # print affected_rows, number_rows
  # (table_name, hive_ddl)=create_hive_ddl_from_csv(csv_file='test/opportunity_no_header.csv2', table_name='tmp2', csv_delimiter='tab')
  # print hive_ddl
  # csv_dump(csv_file='/tmp/opportunity.csv', csv_delimiter='tab', lines=10)
  # (table_name, hive_ddl)=create_hive_ddl_from_csv(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', create_table='Y')
  # execute_remote_hive_query(hive_query='desc top50_ip;')
  # csv_to_hive(csv_file='/tmp/fact_imp_pdp.csv', csv_delimiter='tab', hive_create_table='Y')
  # mysql_to_csv(mysql_host='bidbs', mysql_table='dim_time', mysql_quick='Y')
  # mysql_to_csv(mysql_host='bidbs', mysql_db='bi_staging', mysql_table='userstatsreport', mysql_query='select * from userstatsreport limit 1000;')
  # mysql_to_hive(mysql_host='bidbs', mysql_db='bi_staging', mysql_table='userstatsreport', mysql_query='select * from userstatsreport;',  hive_create_table='Y')
  # hive_to_mysql(hive_table='userstatsreport', hive_db='staging', mysql_db='bi_staging', mysql_table='userstatsreport_hive',  mysql_create_table='Y', max_rows=1000000)
  # count_lines(file='test/census_population.csv', skip_header='N', quiet='N', debug='N')
  #rs = execute_remote_hive_query(hive_db='bi', hive_query="desc dim_listing")
  # print rs
  # rows = count_hive_table_rows(hive_node='namenode2s', hive_db='staging', hive_table='top50_ip', quiet='Y')
  # print rows
  # csv_to_hive_table(csv_file='/tmp/fact_property_view.csv', hive_db='staging', hive_table='fact_property_view_partition', hive_partition="date_int = 20130428")
  # csv_to_hive(csv_file='/tmp/fact_property_view.csv', hive_db='staging', hive_create_table='Y')
  # csv_to_hive(csv_file='/data/tmp/dim_listing.csv', hive_db='staging', hive_table='Y', hive_node='namenode2s', mapred_job_priority = 'HIGH', hive_create_table='Y')
  # csv_to_hive(csv_file='/data/tmp/dim_listing.csv', hive_db='staging', hive_table='Y', hive_node='namenode2s',)
  # csv_to_hive(csv_file='/data/tmp/dim_listing.csv', hive_db='staging', hive_table='fact_property_view_partition', hive_partition="date_int=20130428", hive_create_table='Y')
  # remove_file(file='/tmp/dim_user_tier_2.csv2', debug='Y', quiet='Y')
  # rs = execute_remote_hive_query(hive_node='namenode2s', hive_db='bi', mapred_job_priority="very_high_invalid", hive_query="select count(*) from dim_listing"
  # wait_for_table(mysql_host='bidbs', mysql_db='bi', mysql_table='dim_property', \
  #               mysql_table_update_after = '2013-10-09 03:00', \
  #              sleep_interval = 30, num_retry = 6, stop_at = '12:46', debug='Y', quiet='Y')
  # send_mail( sender = 'pluo@trulia.com', receivers = 'pluo@trulia.com, luo@trulia.com', subject = 'legoo', body_html_file = 'bar.html',  attachment_dir   = '../legoo', attachment_files = ['../show_env.py'], body_text_file = 'csv_dump')
  pass

if __name__ == '__main__':
    main()


