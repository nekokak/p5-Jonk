package t::Utils;
use strict;
use warnings;
use DBI;
use Test::More;
use File::Temp qw(tempdir);

BEGIN {
  eval "use DBD::SQLite";
  plan skip_all => 'needs DBD::SQLite for testing' if $@;
}

sub import {
    strict->import;
    warnings->import;
}

sub _get_schema {
    my $dbh   = shift;
    my $table = shift || "job";

    my $driver = $dbh->{Driver}{Name};
    my $sql;
    if ( $driver eq 'mysql' ) {
        $sql = _read_pod('MySQL');
    } elsif ( $driver eq 'Pg' ) {
        $sql = _read_pod('PostgreSQL');
    } elsif ( $driver eq 'SQLite' ) {
        $sql = _read_pod('SQLite');
    } else {
        Carp::croak "this driver unsupport: $driver";
    }
    $sql =~ s/(CREATE\s+TABLE\s+)job/CREATE TABLE $table/;
    $sql;
}

sub _read_pod {
    my $type = shift;

    open my $fh, '<', './lib/Jonk.pm';
    my $do_read     = 0;
    my $read_schema = 0;
    my $schema='';
    while (<$fh>) {
        my $line = $_;
        if ($line =~ /=head1 SCHEMA/) {
            $do_read=1;
        }
        if ($do_read) {
            if ($line =~ /$type/) {
                $read_schema = 1;
                next;
            }
            if ($read_schema && $line =~ /^=head/) {
                $read_schema = 0;
                last;
            }
            if ($read_schema) {
                $schema .= $line;
                next;
            }
        }
    }
    $schema;
}

my $db;
my $_setup=0;
sub setup {
    my ($class, $table) = @_;

    $db ||= do {
        my $tempdir = tempdir(CLEANUP => 1);
        File::Spec->catfile($tempdir, 'jonk_test.db');
    };

    my $dbh = DBI->connect('dbi:SQLite:'.$db);
    $_setup ? do {1} : do {$dbh->do(_get_schema($dbh, $table)); $_setup=1};
    return $dbh;
}

sub cleanup {
    my ($class, $dbh, $table) = @_;
    $table ||= "job";
    $dbh->do("DROP TABLE $table");
    $_setup=0;
}

1;

