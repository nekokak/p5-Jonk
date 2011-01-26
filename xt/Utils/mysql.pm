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
        my ($class, $table) = @_;
        my $dbh = DBI->connect($mysql->dsn( dbname => "test" ));
        my $schema = t::Utils::_get_schema($dbh, $table);
        $schema =~ s/CREATE TABLE/CREATE TABLE IF NOT EXISTS/;
        $dbh->do($schema);
        $dbh;
    }
}

1;
