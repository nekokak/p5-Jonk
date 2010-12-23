package Jonk::Job;
use strict;
use warnings;

sub new {
    my ($class, $jonk, $job) =@_;
    bless {
        jonk => $jonk,
        job  => $job,
        _completed => 0,
    }, $class;
}

sub completed { $_[0]->{_completed}=1; $_[0]->{jonk}->_completed($_[0]->id) }
sub failed    { $_[0]->{jonk}->_failed($_[0]->id) }

sub id            { $_[0]->{job}->{id}            }
sub func          { $_[0]->{job}->{func}          }
sub arg           { $_[0]->{job}->{arg}           }
sub enqueue_time  { $_[0]->{job}->{enqueue_time}  }
sub grabbed_until { $_[0]->{job}->{grabbed_until} }

sub DESTROY {
    my $self = shift;
    $self->failed unless $self->{_completed};
}
1;
