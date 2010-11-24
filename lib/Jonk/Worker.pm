package Jonk::Worker;
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
        dbh           => $dbh,
        dequeue_query => sprintf('SELECT * FROM %s WHERE func IN (%s) ORDER BY id LIMIT 1 FOR UPDATE',
                             ($opts->{table_name}||'job'),
                             join(', ', map { "'$_'" } @{$opts->{functions}}),
                         ),
    }, $class;
}

sub dequeue {
    my $self = shift;

    my $job = try {
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;
        $self->{dbh}->begin_work;

            my $sth = $self->{dbh}->prepare_cached($self->{dequeue_query});
            $sth->execute(@{$self->{functions}});
            my $row = $sth->fetchrow_hashref;
            $sth->finish;

            if ($row) {
                $sth = $self->{dbh}->prepare_cached('DELETE FROM job WHERE id = ?');
                $sth->execute($row->{id});
                $sth->finish;
            }

        $self->{dbh}->commit;

        return $row;

    } catch {
        Carp::carp("can't get job from job queue database: $_");
        return;
    };

    $job;
}

1;
__END__

=head1 NAME

Jonk::Worker -

=head2 my $jonk = Jonk->new($dbh, $options);

Creates a new Jonk object, and returns the object.

=over 4

=item * $dbh

$dbh is database handle.

=item * $options->{functions}

Key word of job which this Jonk instance looks for.

=item * $options->{table_name}

specific job table name.

Default job table name is `job`.

=back

=head2 my $job_hash_ref = $jonk->dequeue;

dequeue a job from a database.

returns job hashref data.

Please do deserialize if it is necessary. 

=cut

