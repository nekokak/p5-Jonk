package Jonk::Job;
use strict;
use warnings;
use Carp ();

sub new {
    my ($class, $jonk, $job) =@_;
    bless {
        job   => $job,
        _jonk => $jonk,
        _completed => 0,
        _failed    => 0,
        _aborted   => 0,
    }, $class;
}

sub completed {
    my $self = shift;

    if ($self->is_aborted || $self->is_failed) {
        Carp::croak 'job is already (abort|fail)ed.';
    }

    $self->{_completed} = 1;
    $self->{_jonk}->_delete($self->id);
}

sub failed {
    my ($self, $opt) = @_;

    if ($self->is_complated || $self->is_aborted) {
        Carp::croak 'job is already (complate|abort)ed.';
    }

    $self->{_failed} = 1;
    $self->{_jonk}->_failed(
        $self->id => $opt
    );
}

sub abort {
    my $self = shift;

    if ($self->is_completed || $self->is_failed) { 
        Carp::croak 'job is already (complate|fail)ed.';
    }

    $self->{_aborted} = 1;
    $self->{_jonk}->_delete($self->id);
}

sub id            { $_[0]->{job}->{id}            }
sub func          { $_[0]->{job}->{func}          }
sub arg           { $_[0]->{job}->{arg}           }
sub enqueue_time  { $_[0]->{job}->{enqueue_time}  }
sub grabbed_until { $_[0]->{job}->{grabbed_until} }
sub retry_cnt     { $_[0]->{job}->{retry_cnt}     }
sub run_after     { $_[0]->{job}->{run_after}     }
sub priority      { $_[0]->{job}->{priority}      }

sub is_completed { $_[0]->{_completed} }
sub is_failed    { $_[0]->{_failed}    }
sub is_aborted   { $_[0]->{_aborted}   }

sub DESTROY {
    my $self = shift;
    unless ($self->is_completed or $self->is_aborted or $self->is_failed) {
        Carp::croak "job is not (complete|fail|abor)ed. this job auto failed.";
        $self->failed;
    }
}

1;

__END__

=head1 METHODS

=head2 $job->failed([$options]);

=over 4

=item * $options->{retry_delay}

job retry delay sec.

Default 60.

=back

=cut

