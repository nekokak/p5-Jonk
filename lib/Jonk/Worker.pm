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
        dbh       => $dbh,
        functions => ($opts->{functions}||[]),
    }, $class;
}

sub dequeue {
    my $self = shift;

    my $sql = sprintf 'SELECT * FROM job WHERE func IN (%s) ORDER BY id LIMIT 1 FOR UPDATE', join( ", ", ("?") x @{$self->{functions}} );

    my $job = try {
        $self->{dbh}->begin_work;

            my $sth = $self->{dbh}->prepare_cached($sql);
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

=back

=head2 my $job_hash_ref = $jonk->dequeue;

dequeue a job from a database.

returns job hashref data.

Please do deserialize if it is necessary. 

=cut

