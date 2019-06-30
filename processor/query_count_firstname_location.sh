#!/usr/bin/env bash

if  [[ "$#" -ne 3 ]]; then
    echo "Usage: query_count_location table_name firstname location ";
    exit 1;
fi

echo "select sum (count) as result from \"$1\" where location = '$3' and first_name='$2'" > query.sql
/usr/hdp/current/phoenix-client/bin/sqlline.py sandbox-hdp.hortonworks.com query.sql
rm -f query.sql
