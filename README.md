vertica-compression
===================

A Python version of the compression ratio estimation script (located at  `scripts/collect_diag_dump.sh` in the Vertica installation directory).

The script deviates from `collect_diag_dump.sh` in a few minor ways:

1.  Stores output into a table for further analysis with SQL.
2.  Maintains a history of estimates for comparison over time.

This script uses the same sampling method used by `collect_diag_dump.sh` to estimate average uncompressed bytes per row. It also uses the same query logic to calculate compressed sizes within Vertica, though I've modified the query to support the stored history.


### Usage

    usage: compression.py [-h] [--driver DRIVER] [--host HOST] 
                          [--user USER] [--tmpdir TMPDIR]
                          dbname

    positional arguments:
      dbname           database name

    optional arguments:
      -h, --help       show this help message and exit
      --driver DRIVER  odbc driver (default: HPVertica)
      --host HOST      database host (default: localhost)
      --user USER      database user (default: current user)
      --tmpdir TMPDIR  tempfile location (default: cwd)


### Requirements

The script uses pyodbc for interaction with Vertica. Rather than relying on an odbc.ini file, it builds a connection string from arguments passed so that it can re-use the credentials in the sampling calls to `vsql`.

The included `odbcinst.ini` contains the entry for the script's default HPVertica driver. 


### Gotchas

I encountered [pyodbc issue 78](https://code.google.com/p/pyodbc/issues/detail?id=78) using the packaged pyodbc on CentOS. This was corrected by uninstalling the package and doing `pip install pyodbc` to get a version newer than 2.1.7. On CentOS you'll need to install the `unixODBC-devel` package so that pip can compile pyodbc.
