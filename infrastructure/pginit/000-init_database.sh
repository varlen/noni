#!/bin/sh -e

database=mydb

psql --variable=ON_ERROR_STOP=1 --username "pguser" <<-EOSQL
    CREATE DATABASE "$database" OWNER = pguser;
    GRANT ALL PRIVILEGES ON DATABASE "$database" TO pguser;
EOSQL