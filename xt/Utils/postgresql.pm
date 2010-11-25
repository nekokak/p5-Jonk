package xt::Utils::postgresql;

use strict;
use warnings;
use Test::postgresql;
use Test::More;
use t::Utils;

my $pgsql = Test::postgresql->new
    or plan skip_all => $Test::postgresql::errstr;

{
    no warnings "redefine";
    sub t::Utils::setup {
        my $dbh = DBI->connect($pgsql->dsn);
        $dbh->do(q{
            CREATE TABLE job (
                id           SERIAL PRIMARY KEY ,
                func         TEXT NOT NULL,
                arg          BYTEA,
                enqueue_time TIMESTAMP NOT NULL
            )
        });
        $dbh;
    }
}

1;
