package Jonk::Worker;
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
        dbh            => $dbh,
        find_job_query => sprintf('SELECT * FROM %s WHERE func IN (%s) ORDER BY id LIMIT %s',
                             ($opts->{table_name}||'job'),
                             join(', ', map { "'$_'" } @{$opts->{functions}}),
                             ($opts->{job_find_size}||50),
                         ),
        dequeue_query  => sprintf('DELETE FROM %s WHERE id = ?', ($opts->{table_name}||'job')),
    }, $class;
}

sub dequeue {
    my $self = shift;

    my $job;
    try {
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;

        my $sth = $self->{dbh}->prepare($self->{find_job_query});
        $sth->execute() or dir $self->{dbh}->errstr;

        while (my $row = $sth->fetchrow_hashref) {
            my $del_sth = $self->{dbh}->prepare_cached($self->{dequeue_query});
            $del_sth->execute($row->{id});
            $del_sth->finish;

            if ($del_sth->rows) {
                $job = $row;
                last;
            }
        }

        $sth->finish;
    } catch {
        Carp::carp("can't get job from job queue database: $_");
    };

    $job;
}

1;
__END__

=head1 NAME

Jonk::Worker - get a job data class.

=head2 my $jonk = Jonk::Worker->new($dbh, $options);

Creates a new Jonk object, and returns the object.

=over 4

=item * $dbh

$dbh is database handle.

=item * $options->{functions}

Key word of job which this Jonk instance looks for.

=item * $options->{table_name}

specific job table name.

Default job table name is `job`.

=item * $options->{job_find_size}

specific lookup job record size.

Default 50.

=back

=head2 my $job_hash_ref = $jonk->dequeue;

dequeue a job from a database.

returns job hashref data.

Please do deserialize if it is necessary. 

=cut

