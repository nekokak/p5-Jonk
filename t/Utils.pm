package t::Utils;
use strict;
use warnings;
use DBI;
use Pod::Simple::SimpleTree;

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

    my $pod_tree = Pod::Simple::SimpleTree->new->parse_file('./lib/Jonk.pm')->root;
    my $read_schema = 0;
    for my $row (@$pod_tree) {

        unless ($read_schema) {
            next unless ref($row) eq 'ARRAY';
            next unless $row->[0] eq 'head2';
            if ($row->[2] eq $type) {
                $read_schema = 1;
            }
            next;
        }

        return $row->[2];
    }
}

sub setup {
    my ($class, $table) = @_;
    my $dbh = DBI->connect('dbi:SQLite:');
    $dbh->do(_get_schema($dbh, $table));
    $dbh;
}

sub cleanup {
    my ($class, $dbh, $table) = @_;
    $table ||= "job";
    $dbh->do("DROP TABLE $table");
}

1;

