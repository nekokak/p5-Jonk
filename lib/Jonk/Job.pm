package Jonk::Job;
use strict;
use warnings;
use Carp ();

sub new {
    my ($class, $jonk, $job) =@_;
    bless {
        job        => $job,
        _jonk      => $jonk,
        _completed => 0,
        _failed    => 0,
        _aborted   => 0,
    }, $class;
}

sub completed {
    my $self = shift;

    Carp::croak 'job is already failed.'  if $self->is_failed;
    Carp::croak 'job is already aborted.' if $self->is_aborted;

    $self->{_completed} = 1;
    $self->{_jonk}->_delete($self->id);
}

sub failed {
    my ($self, $opt) = @_;

    Carp::croak 'job is already complated.' if $self->is_completed;
    Carp::croak 'job is already aborted.'   if $self->is_aborted;

    $self->{_failed} = 1;
    $self->{_jonk}->_failed(
        $self->id => $opt
    );
}

sub aborted {
    my $self = shift;

    Carp::croak 'job is already complated.' if $self->is_completed;
    Carp::croak 'job is already failed.'    if $self->is_failed;

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
        Carp::cluck "job is not (complete|fail|abor)ed. this job auto failed.";
        $self->failed;
    }
}

1;

__END__

__END__

=head1 NAME

Jonk::Job - Jonk job class.

=head1 SYNOPSIS

    my $job = $jonk->lookup_job($job_id);
    $job->arg;
    $job->completed; # complete and delete job
    $job->failed;    # failed and update job for retry
    $job->aborted;   # failed and delete job

=head1 METHODS

=head2 $job->id           

get job id.

=head2 $job->func         

get job function name

=head2 $job->arg.         

get job argument.

=head2 $job->enqueue_time 

get job enqueued time.

=head2 $job->grabbed_until

get job grabbed until time.

=head2 $job->retry_cnt    

get job retried count

=head2 $job->run_after    

get job run after time

=head2 $job->priority     

get job priority num

=head2 $job->completed

completed job.

and delete job data from database.

=head2 $job->failed([$options]);

failed job.
set error message and delete job data from database.

=over 4

=item * $options->{retry_delay}

job retry delay sec.

Default 60 sec.

=back

=head2 $job->aborted

aborted job.
set error message and delete job data from database.

=head2 $job->is_failed

=head2 $job->is_aborted

=cut

