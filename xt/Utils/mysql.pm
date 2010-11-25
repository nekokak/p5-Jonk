package xt::Utils::mysql;

use strict;
use warnings;
use Test::mysqld;
use Test::More;
use t::Utils;

my $mysql = Test::mysqld->new
    or plan skip_all => $Test::mysqld::errstr;

{
    no warnings "redefine";
    sub t::Utils::setup {
        my $dbh = DBI->connect($mysql->dsn( dbname => "test" ));
        $dbh->do(q{
           CREATE TABLE job (
               id           int(10) unsigned NOT NULL auto_increment,
               func         varchar(255)     NOT NULL,
               arg          MEDIUMBLOB,
               enqueue_time DATETIME         NOT NULL,
               primary key ( id )
           ) ENGINE=InnoDB
        });
        $dbh;
    }
}

1;
