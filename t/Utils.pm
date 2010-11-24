package t::Utils;
use strict;
use warnings;
use Test::mysqld;
use JSON::XS;
use DBI;

sub setup {
    my ($class, %config) = @_;

    my $mysqld;
    if (my $json = $ENV{TEST_MYSQLD}) {
        my $obj = decode_json $json;
        $mysqld = bless $obj, 'Test::mysqld';
    }
    else {
        $mysqld = Test::mysqld->new(my_cnf => {
            'skip-networking' => '',
            %config,
        }) or die $Test::mysqld::errstr;
    }

    return $mysqld;
}

sub cleanup {
    my ($class, $mysqld) = @_;
    my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'), '', '', {
        AutoCommit => 1,
        RaiseError => 1,
    });
    $dbh->do('TRUNCATE TABLE job');
}

1;

