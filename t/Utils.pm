package t::Utils;
use strict;
use warnings;
use DBI;

sub setup {
    my $dbh = DBI->connect('dbi:SQLite:');
    $dbh->do(q{
        CREATE TABLE job (
            id           INTEGER PRIMARY KEY ,
            func         text,
            arg          text,
            enqueue_time text
        )
    });
    $dbh;
}

sub cleanup {
    my ($class, $dbh) = @_;
    $dbh->do('DROP TABLE job');
}

1;

