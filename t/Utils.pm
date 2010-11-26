package t::Utils;
use strict;
use warnings;
use DBI;
use Pod::Simple::SimpleTree;

sub _get_schema {
    my $dbh = shift;

    my $driver = $dbh->{Driver}{Name};
    if ( $driver eq 'mysql' ) {
        return _read_pod('MySQL');
    } elsif ( $driver eq 'Pg' ) {
        return _read_pod('PostgreSQL');
    } elsif ( $driver eq 'SQLite' ) {
        return _read_pod('SQLite');
    } else {
        Carp::croak "this driver unsupport: $driver";
    }
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
    my $dbh = DBI->connect('dbi:SQLite:');
    $dbh->do(_get_schema($dbh));
    $dbh;
}

sub cleanup {
    my ($class, $dbh) = @_;
    $dbh->do('DROP TABLE job');
}

1;

