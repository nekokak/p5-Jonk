package Jonk::Job;
use strict;
use warnings;

sub new {
    my ($class, $jonk, $job) =@_;
    bless {
        jonk => $jonk,
        job  => $job,
    }, $class;
}

sub dequeue { $_[0]->{jonk}->_dequeue($_[0]->id) }

sub id            { $_[0]->{job}->{id}            }
sub func          { $_[0]->{job}->{func}          }
sub arg           { $_[0]->{job}->{arg}           }
sub enqueue_time  { $_[0]->{job}->{enqueue_time}  }
sub grabbed_until { $_[0]->{job}->{grabbed_until} }

1;
