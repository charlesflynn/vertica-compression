#!/bin/env python

import os
import sys
import argparse
import subprocess
import getpass
import pyodbc


def get_conn(args):
    params = (args.driver, args.host, args.dbname, args.user, args.passwd)
    connstr = "DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (params)
    try:
        return pyodbc.connect(connstr)
    except pyodbc.Error as e:
        sys.exit("Couldn't connect: {0}".format(e.args))


def create_table(cnxn):
    cursor = cnxn.cursor()
    if cursor.tables(table='diag_compression').fetchone():
        cursor.close()
        return
    else:
        cursor.execute("""
                        CREATE TABLE diag_compression
                        (
                            table_name      VARCHAR(400),
                            vertica_bytes   INT,
                            vertica_rows    INT,
                            vertica_rowsize FLOAT,
                            proj_count      INT,
                            proj_type       CHAR(1),
                            sample_bytes    INT,
                            sample_rows     INT,
                            sample_rowsize  FLOAT,
                            compression     FLOAT,
                            diag_date       TIMESTAMP
                        ); 
                       """)
    cursor.close()
    return


def create_view(cnxn):
    cursor = cnxn.cursor()
    if cursor.tables(table='diag_projections').fetchone():
        cursor.close()
        return
    else:
    # query modified from collect_diag_dump.sh  
        cursor.execute("""
                        CREATE VIEW diag_projections AS
                            SELECT anchor_table_schema,
                            anchor_table_name,
                            projection_name,
                            SUM(used_bytes) AS used_bytes,
                            CASE INSTR(RIGHT(projection_name, 9), '_node')
                                WHEN 0 THEN projection_name
                                ELSE SPLIT_PART(projection_name,'_node', 1)
                            END pjname_short,
                            CASE INSTR(RIGHT(projection_name, 9),'_node')
                                WHEN 0 THEN 'S'
                                ELSE  'R'
                            END pjtype,
                            --use projection_name to identify replicated 
                            --versus segmented projections
                            --replicated contain _node0001 in projection name
                            CASE INSTR(RIGHT(projection_name,9),'_node')
                                WHEN 0 THEN sum(row_count)  --segmented	
                                ELSE max(row_count)         --replicated
                            END row_count
                            FROM projection_storage
                            GROUP BY projection_name, 
                                     anchor_table_schema, 
                                     anchor_table_name;
                       """)
    cursor.close()
    return


def insert_stats(cnxn):
    cursor = cnxn.cursor()
    cursor.execute("""
                    INSERT INTO diag_compression
                    SELECT anchor_table_schema || '.' || anchor_table_name AS table_name,
                        SUM(used_bytes) AS vertica_bytes,
                        MAX(ROW_COUNT) AS vertica_rows,
                        SUM(used_bytes)/MAX(ROW_COUNT) AS vertica_rowsize,
                        COUNT(DISTINCT pjname_short) AS proj_count,
                        CASE
                            WHEN MIN(pjtype) <> MAX(pjtype) THEN 'M'
                            ELSE MIN(pjtype)
                        END proj_type,
                        NULL AS sample_bytes,
                        NULL AS sample_rows,
                        NULL AS sample_rowsize,
                        NULL AS compression,
                        NOW() AS diag_date
                    FROM diag_projections
                    GROUP BY anchor_table_schema,
                             anchor_table_name;
                   """)
    cnxn.commit()
    ret = cursor.rowcount
    cursor.close()
    return ret


def get_tables(cnxn):
    cursor = cnxn.cursor()
    cursor.execute("""
                    SELECT table_name, vertica_rowsize
                    FROM diag_compression
                    WHERE sample_bytes IS NULL
                        AND sample_rows IS NULL;
                   """)
    ret = cursor.fetchall()
    cursor.close()
    return ret


def do_sample(table, args):
    # same sampling query and method used by collect_diag_dump.sh
    # note that "SELECT *" is usually a performance no-no in Vertica
    q = 'SELECT * FROM {0} WHERE RANDOM() < 0.05 LIMIT 10000000'.format(table)
    outfile = os.path.join(args.tmpdir, 'vsqlout.tmp')
    vsql_args = ['/opt/vertica/bin/vsql', '-At',
                    '-h', args.host,
                    '-d', args.dbname,
                    '-U', args.user,
                    '-w', args.passwd,
                    '-o', outfile, 
                    '-c', q]
    subprocess.call(vsql_args)
    ret = {}
    ret['bytes'] = os.path.getsize(outfile)
    ret['rows'] = sum(1 for line in open(outfile))
    os.remove(outfile)
    return ret 


def update_stats(cnxn, table, vertica_rowsize=0, sample_bytes=0, sample_rows=0):
    cursor = cnxn.cursor()
    if sample_rows == 0:
        sample_rowsize = 0
    else:
        sample_rowsize = sample_bytes/sample_rows

    if vertica_rowsize == 0:
        compression = 0
    else:
        compression = sample_rowsize/vertica_rowsize

    cursor.execute("""
                    UPDATE diag_compression
                    SET sample_bytes = ?,
                        sample_rows = ?,
                        sample_rowsize = ?,
                        compression = ?
                    WHERE TABLE_NAME = ?
                    AND sample_bytes IS NULL
                    AND sample_rows IS NULL;
                   """, [sample_bytes, sample_rows, sample_rowsize, compression, table])
    cnxn.commit()
    cursor.close()
    return compression


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dbname', help='database name')
    parser.add_argument('--driver', 
            default='HPVertica', help='odbc driver (default: HPVertica)')
    parser.add_argument('--host', 
            default='localhost', help='database host (default: localhost)')
    parser.add_argument('--user', 
            default=getpass.getuser(), help='database user (default: current user)')
    parser.add_argument('--tmpdir', 
            default=os.getcwd(), help='tempfile location (default: cwd)')

    args = parser.parse_args()
    if not os.path.exists(args.tmpdir):
        sys.exit('{0} is not a valid directory'.format(args.tmpdir))

    args.passwd = getpass.getpass()
    cnxn = get_conn(args)
    create_table(cnxn)
    create_view(cnxn)
    newrows = insert_stats(cnxn)
    print 'Inserted {0} rows into diag_compression'.format(newrows)

    rows = get_tables(cnxn)
    for row in rows:
        table, vertica_rowsize = row
        sample = do_sample(table, args)
        compression = update_stats(cnxn, table, 
                                   vertica_rowsize=vertica_rowsize,
                                   sample_bytes=sample['bytes'], 
                                   sample_rows=sample['rows'])
        print '{0} compression for table {1}'.format(compression, table)
    cnxn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
