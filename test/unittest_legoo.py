#!/usr/bin/python

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import csv, os
import legoo
import unittest

# workflow: load csv to mysql; dump mysql to hive table, dump hive table back to mysql 
class TestSequenceFunctions(unittest.TestCase):
    def setUp(self):
        self.csv_file='census_population.csv'         # sample csv file
        self.csv_file_no_header='census_population_no_header.csv' # sample csv file without header

        # trulia enviroment. update to your system settings accordingly
        # trulia HIVE env
        self.hive_node='hive-cdh4-prod'
        self.hive_db='staging'
        self.hive_table='tmp_hive_census'
        self.hive_export_csv='tmp_census_population_hive.csv' 

        # trulia MySQL env
        self.mysql_host='bidbs'
        self.mysql_db='bi_staging'
        self.mysql_table='tmp_census'
        self.mysql_export_csv='tmp_census_population.csv'  

    def test002_count_lines(self):
         r=legoo.count_lines(file=self.csv_file, skip_header='N')
         self.assertEqual(r, 58)

    def test004_count_lines_skip_header(self):
         r=legoo.count_lines(file=self.csv_file, skip_header='Y')
         self.assertEqual(r, 57)

    def test006_csv_dump(self):
        # test csv_dump not throw exception
        try:
            legoo.csv_dump(csv_file=self.csv_file, csv_delimiter=',', lines=1)
        except:
            self.fail("csv_dump failed")

    def test008_wait_for_file_01(self):
        # test csv_dump not throw exception
        try:
            legoo.wait_for_file(num_retry=3, sleep_interval=5,  file=self.csv_file)
        except:
            self.fail("wait_for_file failed")

    # test below are trulia specific due to dependency on MySQL and Hive
    def test100_execute_mysql_query(self):
        # drop target table if exists
        ddl = "drop table if exists %s.%s" % ( self.mysql_db, self.mysql_table)
        try:
            legoo.execute_mysql_query(mysql_host     = self.mysql_host, \
                                      mysql_db       = self.mysql_db, \
                                      mysql_query    = ddl)
        except:
            self.fail("execute_mysql_query failed")

    def test102_csv_to_mysql(self):
        # test csv_to_mysql not throw exception
        try:
            legoo.csv_to_mysql(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                               mysql_table = self.mysql_table, mysql_create_table='Y', \
                               csv_file = self.csv_file, csv_header = 'Y', \
                               csv_delimiter=',')
        except:
            self.fail("csv_to_mysql with option mysql_create failed") 

    def test104_csv_to_mysql_truncate_table(self):
        try:
            legoo.csv_to_mysql(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                               mysql_table = self.mysql_table, mysql_truncate_table='Y', \
                               csv_file = self.csv_file, csv_header = 'Y', \
                               csv_delimiter=',')
        except:
            self.fail("csv_to_mysql with option mysql_truncate_table failed") 

    def test106_csv_to_mysql_table(self):
        try:
            legoo.csv_to_mysql_table(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                     mysql_table = self.mysql_table, csv_file = self.csv_file, \
                                     csv_header = 'Y', csv_delimiter=',')
        except:
            self.fail("csv_to_mysql_table failed") 

    def test108_mysql_to_csv(self):
        try:
            legoo.mysql_to_csv(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                               mysql_table = self.mysql_table, \
                               csv_dir = '.', csv_file = self.mysql_export_csv)
        except:
            self.fail("mysql_to_csv failed")

    def test110_remove_file(self):
        try:
            legoo.remove_file(file = self.mysql_export_csv)
        except:
            self.fail("remove_file failed")

    def test112_create_mysql_ddl_from_csv(self):
        try:
            legoo.create_mysql_ddl_from_csv(table_name = self.mysql_table, csv_file = self.csv_file, csv_delimiter= ',') 
        except:
            self.fail("create_mysql_ddl_from_csv failed")

    def test114_wait_for_table(self):
        try:
            legoo.wait_for_table(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                 mysql_table = self.mysql_table, num_retry=3, sleep_interval=5)
        except:
            self.fail("wait_for_table failed") 


    # test below are trulia specific due to dependency on MySQL and Hive
    def test200_execute_remote_hive_query(self):
        # drop target table if exists
        ddl = "drop table if exists %s.%s" % ( self.hive_db, self.hive_table)
        legoo.execute_remote_hive_query(hive_node     = self.hive_node, \
                                        hive_db       = self.hive_db, \
                                        hive_query    = ddl)

    def test202_create_hive_ddl_from_csv(self):
        try:
            legoo.create_hive_ddl_from_csv(table_name = self.hive_table, csv_file = self.csv_file, csv_delimiter= ',') 
        except:
            self.fail("create_hive_ddl_from_csv failed")

    def test204_csv_to_hive(self):
        try:
            legoo.csv_to_hive(hive_node = self.hive_node, hive_db = self.hive_db, \
                              hive_table = self.hive_table, hive_create_table='Y', \
                              csv_file = self.csv_file, csv_header = 'Y', \
                              mapred_job_priority='VERY_HIGH', csv_delimiter=',')
        except:
            self.fail("csv_to_hive failed") 

    def test205_csv_to_hive(self):
        try:
            legoo.csv_to_hive(hive_node = self.hive_node, hive_db = self.hive_db, \
                              hive_table = self.hive_table, hive_create_table='N', hive_overwrite = 'Y', \
                              csv_file = self.csv_file, csv_header = 'Y', \
                              mapred_job_priority='VERY_HIGH', csv_delimiter=',')
        except:
            self.fail("csv_to_hive overwrite failed") 


    def test206_csv_to_hive_table(self):
        try:
            legoo.csv_to_hive_table(hive_node = self.hive_node, hive_db = self.hive_db, \
                                    hive_table = self.hive_table, csv_file = self.csv_file, \
                                    mapred_job_priority='VERY_HIGH', csv_header = 'Y', csv_delimiter=',')
        except:
            self.fail("csv_to_hive_table failed")


    def test208_hive_to_csv(self):
        try:
            legoo.hive_to_csv(hive_node = self.hive_node, hive_db = self.hive_db, \
                              mapred_job_priority='VERY_HIGH', hive_table = self.hive_table, \
                              csv_dir = './', csv_file = self.hive_export_csv)
        except:
            self.fail("hive_to_csv failed")

    def test210_count_hive_table_rows(self):
        try:
            legoo.count_hive_table_rows(hive_node = self.hive_node, hive_db = self.hive_db, \
                              mapred_job_priority='VERY_HIGH', hive_table = self.hive_table)
        except:
            self.fail("count_hive_table_rows failed")

    def test212_remove_file(self):
        try:
            legoo.remove_file(file = self.hive_export_csv)
        except:
            self.fail("remove_file failed")

    def test214_csv_to_hive(self):
        try:
            legoo.csv_to_hive(hive_node = self.hive_node, hive_db = self.hive_db, \
                              hive_table = self.hive_table, hive_create_table='N', hive_overwrite = 'N', \
                              csv_file = self.csv_file, csv_header = 'Y', \
                              mapred_job_priority='VERY_HIGH', csv_delimiter=',')
        except:
            self.fail("csv_to_hive append failed") 


    def test216_csv_to_hive(self):
        try:
            legoo.csv_to_hive(hive_node = self.hive_node, hive_db = self.hive_db, \
                              hive_table = self.hive_table, hive_create_table='N', hive_overwrite = 'Y', \
                              csv_file = self.csv_file_no_header, csv_header = 'N', \
                              mapred_job_priority='VERY_HIGH', csv_delimiter=',')
        except:
            self.fail("csv_to_hive no header overwrite failed") 

    def test218_csv_to_hive(self):
        try:
            legoo.csv_to_hive(hive_node = self.hive_node, hive_db = self.hive_db, \
                              hive_table = self.hive_table, hive_create_table='N', hive_overwrite = 'N', \
                              csv_file = self.csv_file_no_header, csv_header = 'N', \
                              mapred_job_priority='VERY_HIGH', csv_delimiter=',')
        except:
            self.fail("csv_to_hive no header append failed") 


    def test300_mysql_to_hive(self):
        try:
            # drop table if exists
            ddl = "drop table if exists %s.%s" % ( self.hive_db, self.hive_table)
            legoo.execute_remote_hive_query(hive_node  = self.hive_node, \
                                            hive_db    = self.hive_db, \
                                            hive_query = ddl)

            legoo.mysql_to_hive(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table, \
                                hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, hive_create_table='Y', \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab')
        except:
            self.fail("mysql_to_hive with option hive_create_table failed") 

    def test302_mysql_to_hive_table(self):
        try:
            # export mysql table to existing hive table
            legoo.mysql_to_hive(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table, \
                                hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab')
        except:
            self.fail("mysql_to_hive failed") 

    def test304_mysql_to_hive_table(self):
        try:
            # export mysql table to existing hive table
            legoo.mysql_to_hive(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table, remove_carriage_return='Y', \
                                hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab')
        except:
            self.fail("mysql_to_hive with option remove_carriage_return failed") 

    def test306_mysql_to_hive_table(self):
        try:
            # export mysql table to existing hive table
            query = "select * from  %s.%s limit 10" % ( self.mysql_db, self.mysql_table)
            legoo.mysql_to_hive(mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table, mysql_query = query, \
                                hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab')
        except:
            self.fail("mysql_to_hive with option mysql_query failed") 

    def test320_hive_to_mysql(self):
        try:
            ddl = "drop table if exists %s.%s" % ( self.mysql_db, self.mysql_table)
            legoo.execute_mysql_query(mysql_host     = self.mysql_host, \
                                      mysql_db       = self.mysql_db, \
                                      mysql_query    = ddl)
            legoo.hive_to_mysql(hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab', \
                                mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_create_table='Y', mysql_table = self.mysql_table)
        except:
            self.fail("hive_to_mysql with optiion mysql_create_table failed") 

    def test322_hive_to_mysql(self):
        try:
            legoo.hive_to_mysql(hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab', \
                                mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table)
        except:
            self.fail("hive_to_mysql failed") 

    def test324_hive_to_mysql(self):
        try:
            query = "select * from  %s.%s limit 10" % ( self.hive_db, self.hive_table)
            legoo.hive_to_mysql(hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, hive_query = query, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab', \
                                mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_table = self.mysql_table)
        except:
            self.fail("hive_to_mysql with option mysql_query failed") 

    def test326_hive_to_mysql(self):
        try:
            query = "select * from  %s.%s limit 10" % ( self.hive_db, self.hive_table)
            legoo.hive_to_mysql(hive_node = self.hive_node, hive_db = self.hive_db, \
                                hive_table = self.hive_table, hive_query = query, \
                                mapred_job_priority='VERY_HIGH', csv_delimiter='tab', \
                                mysql_host = self.mysql_host, mysql_db = self.mysql_db, \
                                mysql_truncate_table = 'Y', mysql_table = self.mysql_table)
        except:
            self.fail("hive_to_mysql with option mysql_truncate_table failed") 

    def test999_clean_up(self):
        # drop MySQL test table 
        ddl = "drop table if exists %s.%s" % ( self.mysql_db, self.mysql_table)
        legoo.execute_mysql_query(mysql_host     = self.mysql_host, \
                                  mysql_db       = self.mysql_db, \
                                  mysql_query    = ddl)

        # drop HIVE test table 
        ddl = "drop table if exists %s.%s" % ( self.hive_db, self.hive_table)
        legoo.execute_remote_hive_query(hive_node  = self.hive_node, \
                                        hive_db    = self.hive_db, \
                                        hive_query = ddl)

        # remove test csv from hive export
        legoo.remove_file(file = self.hive_export_csv)
        # remove test csv from mysql export
        legoo.remove_file(file = self.mysql_export_csv)

if __name__ == '__main__':
    unittest.main()



