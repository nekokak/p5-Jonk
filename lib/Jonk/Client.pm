package Jonk::Client;
use strict;
use warnings;
use Carp ();
use Try::Tiny;

sub new {
    my ($class, $dbh, $opts) = @_;

    unless ($dbh) {
        Carp::croak('missing job queue database handle.');
    }

    bless {
        dbh           => $dbh,
        enqueue_query => sprintf('INSERT INTO %s (func, arg, enqueue_time) VALUES (?,?,?)', ($opts->{table_name}||'job')),
        enqueue_time_callback => ($opts->{enqueue_time_callback}||sub{
            my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
            return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
        }),
    }, $class;
}

sub enqueue {
    my ($self, $func, $arguments) = @_;

    my $job_id;
    try {
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;
        my $sth = $self->{dbh}->prepare_cached($self->{enqueue_query});
        $sth->execute($func, $arguments->{arg}, $self->{enqueue_time_callback}->());
        $job_id = $self->_insert_id($self->{dbh});
        $sth->finish;
    } catch {
        Carp::carp("can't enqueue for job queue database: $_");
    };

    $job_id;
}

sub _insert_id {
    my ($self, $dbh) = @_;

    my $driver = $dbh->{Driver}{Name};
    if ( $driver eq 'mysql' ) {
        return $dbh->{mysql_insertid};
    } elsif ( $driver eq 'Pg' ) {
        return $dbh->last_insert_id( undef, undef, undef, undef,{ sequence => join( '_', 'job', 'id', 'seq' ) } );
    } elsif ( $driver eq 'SQLite' ) {
        return $dbh->func('last_insert_rowid');
    } else {
        Carp::croak "Don't know how to get last insert id for $driver";
    }
}

1;
__END__

=head1 NAME

Jonk::Client - job enqueue client class.

=head2 my $jonk = Jonk::Client->new($dbh, $options);

Creates a new Jonk object, and returns the object.

=over 4

=item * $dbh

$dbh is database handle.

=item * $options->{table_name}

specific job table name.

Default job table name is `job`.

=item * $options->{enqueue_time_callback}

specific enqueue_time creation callback.

Default local time create.

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

=back

=cut

