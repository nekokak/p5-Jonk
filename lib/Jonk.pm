package Jonk;
use strict;
use warnings;
use Carp;
use Try::Tiny;

our $VERSION = '0.01';

sub new {
    my ($class, $dbh, $opts) = @_;

    unless ($dbh) {
        Carp::croak('missing job queue database handle.');
    }

    bless {
        dbh   => $dbh,
        funcs => ($opts->{funcs}||[]),
    }, $class;
}

sub enqueue {
    my ($self, $func, $arg) = @_;

    my $job_id = try {
        my $sth = $self->{dbh}->prepare_cached('INSERT INTO job (func, arg, enqueue_time) VALUES (?,?,?)');

        my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
        my $time = sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
        $sth->execute($func, $arg, $time);
        $self->_insert_id($self->{dbh}, $sth);
    } catch {
        Carp::carp("can't enqueue for job queue database: $_");
    };

    $job_id;
}

sub add_funcs {
    my ($self, $func) = @_;
    push @{$self->{funcs}}, $func;
}

sub dequeue {
    my $self = shift;

    my $sql = sprintf 'SELECT * FROM job WHERE func IN (%s) ORDER BY id LIMIT 1 FOR UPDATE', join( ", ", ("?") x @{$self->{funcs}} );

    my $job = try {
        $self->{dbh}->begin_work;

            my $sth = $self->{dbh}->prepare_cached($sql);
            $sth->execute(@{$self->{funcs}});
            my $row = $sth->fetchrow_hashref;

            $sth = $self->{dbh}->prepare_cached('DELETE FROM job WHERE id = ?');
            $sth->execute($row->{id});

        $self->{dbh}->commit;

        return +{
            func => $row->{func},
            arg  => $row->{arg},
        };

    } catch {
        Carp::carp("can't get job from job queue database: $_");
    };

    $job;
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

Jonk - simple job tank manager.

=head1 SYNOPSIS

    use Jonk;
    
    my $dbh = DBI->connect(...);
    # enqueue job
    {
        my $jonk = Jonk->new($dbh);
        $jonk->enqueue('MyWorker', 'arg');
    }

    # dequeue job
    {
        my $jonk = Jonk->new($dbh, {funcs => ['MyWorker']});
        my $job = $jonk->dequeue;
        print $job->{func}; # MyWorker
        print $job->{arg};  # arg
    }

=head1 DESCRIPTION

Jonk is simple job tanking system.

Job is saved and taken out. Besides, nothing is done.

You may use Jonk to make original Job Queuing System.

Jonk is a META Job Queuing System.

=head1 METHODS

=head2 my $jonk = Jonk->new($dbh, $options);

Creates a new Jonk object, and returns the object.

=over 4

=item * $dbh

$dbh is database handle.

=item * $options->{funcs}

Key word of job which this Jonk instance looks for.

=back

=head2 my $job_id = $jonk->enqueue($func, $arg);

enqueue a job to a database.
returns job.id.

=over 4

=item * $func

=item * $arg

job argument data.
serialize is not done in Jonk. 
Please pass data that does serialize if it is necessary. 

=back

=head2 my $job_hash_ref = $jonk->dequeue;

dequeue a job from a database.

returns job hashref data.

Please do deserialize if it is necessary. 

=head2 $jonk->add_funcs($func);

The key word to do dequeue is set. 

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

