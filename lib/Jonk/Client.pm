package Jonk::Client;
use strict;
use warnings;
use Carp;
use Try::Tiny;

sub new {
    my ($class, $dbh, $opts) = @_;

    unless ($dbh) {
        Carp::croak('missing job queue database handle.');
    }

    bless {
        dbh => $dbh,
    }, $class;
}

sub enqueue {
    my ($self, $func, $arguments) = @_;

    my $job_id;
    try {
        my $sth = $self->{dbh}->prepare_cached('INSERT INTO job (func, arg, enqueue_time) VALUES (?,?,?)');
        $sth->execute($func, $arguments->{arg}, $arguments->{time});
        $job_id = $self->_insert_id($self->{dbh}, $sth);
        $sth->finish;
    } catch {
        Carp::carp("can't enqueue for job queue database: $_");
    };

    $job_id;
}

sub _insert_id {
    my ($self, $dbh, $sth) = @_;

    my $driver = $dbh->{Driver}{Name};
    if ( $driver eq 'mysql' ) {
        return $dbh->{mysql_insertid};
    } elsif ( $driver eq 'Pg' ) {
        return $dbh->last_insert_id( undef, undef, undef, undef,{ sequence => join( '_', 'job', 'id', 'seq' ) } );
    } else {
        Carp::croak "Don't know how to get last insert id for $driver";
    }
}

1;
__END__

=head1 NAME

Jonk::Client - 

=head2 my $jonk = Jonk->new($dbh);

Creates a new Jonk object, and returns the object.

=over 4

=item * $dbh

$dbh is database handle.

=back

=head2 my $job_id = $jonk->enqueue($func, $arguments);

enqueue a job to a database.
returns job.id.

=over 4

=item * $func

=item * $arguments->{arg}

job argument data.
serialize is not done in Jonk. 
Please pass data that does serialize if it is necessary. 

=item * $arguments->{time}

job enqueue time.
format: YYYY-MM-DD HH:MM::SS

=back

=cut

