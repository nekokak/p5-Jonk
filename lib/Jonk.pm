package Jonk;
use strict;
use warnings;
use Jonk::Job;
use Try::Tiny;
use Carp ();

our $VERSION = '0.06';

sub new {
    my ($class, $dbh, $opts) = @_;

    unless ($dbh) {
        Carp::croak('missing job queue database handle.');
    }

    my $table_name = $opts->{table_name} || "job";

    bless {
        dbh           => $dbh,

        enqueue_query => sprintf('INSERT INTO %s (func, arg, enqueue_time, grabbed_until) VALUES (?,?,?,0)', $table_name),
        enqueue_time_callback => ($opts->{enqueue_time_callback}||sub{
            my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
            return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
        }),

        lookup_job_query => sprintf('SELECT * FROM %s WHERE id = ? AND grabbed_until <= ?', $table_name),
        find_job_query   => sprintf('SELECT * FROM %s WHERE func IN (%s) AND grabbed_until <= ? ORDER BY id LIMIT %s',
                             $table_name,
                             join(', ', map { "'$_'" } @{$opts->{functions}}),
                             ($opts->{job_find_size}||50),
                         ),
        grab_job_query   => sprintf('UPDATE %s SET grabbed_until = ? WHERE id = ? AND grabbed_until = ?', $table_name),

        dequeue_query    => sprintf('DELETE FROM %s WHERE id = ?', $table_name),

        table_name    => $table_name,
        _errstr       => undef,
    }, $class;
}

sub errstr {$_[0]->{_errstr}}

sub enqueue {
    my ($self, $func, $arg) = @_;

    my $job_id;
    try {
        $self->{_errstr} = undef;
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;

        my $sth = $self->{dbh}->prepare_cached($self->{enqueue_query});
        $sth->bind_param(1, $func);
        $sth->bind_param(2, $arg, _bind_param_attr($self->{dbh}));
        $sth->bind_param(3, $self->{enqueue_time_callback}->());
        $sth->execute();

        $job_id = $self->{dbh}->last_insert_id("","",$self->{table_name},"");
        $sth->finish;
    } catch {
        $self->{_errstr} = "can't enqueue for job queue database: $_"
    };

    $job_id;
}

sub _bind_param_attr {
    my $dbh = shift;

    my $driver = $dbh->{Driver}{Name};
    if ( $driver eq 'Pg' ) {
        return { pg_type => DBD::Pg::PG_BYTEA() };
    } elsif ( $driver eq 'SQLite' ) {
        return DBI::SQL_BLOB();
    }
    return;
}

sub lookup {
    my ($self, $job_id) = @_;

    my $job;
    try {
        $self->{_errstr} = undef;
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;

        my $time = time;
        my $sth;
        if ($job_id) {
            $sth = $self->{dbh}->prepare_cached($self->{lookup_job_query});
            $sth->execute($job_id, $time);
        } else {
            $sth = $self->{dbh}->prepare_cached($self->{find_job_query});
            $sth->execute($time);
        }

        while (my $row = $sth->fetchrow_hashref) {
            # TODO: grab_a_job method.
            my $grab_sth = $self->{dbh}->prepare_cached($self->{grab_job_query});
            $grab_sth->execute((time + 60), $row->{id}, $row->{grabbed_until});
            $grab_sth->finish;

            if ($grab_sth->rows) {
                $job = Jonk::Job->new($self => $row);
                last;
            }
        }

        $sth->finish;
    } catch {
        $self->{_errstr} = "can't get job from job queue database: $_";
    };

    $job;
}

sub _dequeue {
    my ($self, $job_id) = @_;

    try {
        my $sth = $self->{dbh}->prepare_cached($self->{dequeue_query});
        $sth->execute($job_id);
        $sth->finish;
        return $sth->rows;
    } catch {
        $self->{_errstr} = "can't dequeue job from job queue database: $_";
        return;
    };
}

1;

__END__

=head1 NAME

Jonk - simple job tank manager.

=head1 SYNOPSIS

    use DBI; 
    use Jonk;
    my $dbh = DBI->connect(...);
    my $jonk = Jonk->new($dbh, {functions => ['MyWorker']});
    # enqueue job
    {
        $jonk->enqueue('MyWorker', 'arg');
    }

    # dequeue job
    {
        my $job = $jonk->lookup;
        print $job->{func}; # MyWorker
        print $job->{arg};  # arg
        $job->dequeue;
    }

=head1 DESCRIPTION

Jonk is simple job tanking system.

Job is saved and taken out. Besides, nothing is done.

You may use Jonk to make original Job Queuing System.

=head1 METHODS

=head2 my $jonk = Jonk::Worker->new($dbh, [$options]);

Creates a new Jonk object, and returns the object.

$options is an optional settings.

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

=head2 my $job_id = $jonk->enqueue($func, $arg);

enqueue a job to a database.
returns job.id.

=over 4

=item * $func

specific your worker funcname.

=item * $arg

job argument data.

serialize is not done in Jonk. 

Please pass data that does serialize if it is necessary. 

=back

=head2 my $job_hash_ref = $jonk->lookup([$job_id]);

lookup a job from a database.

returns job hashref data.

Please do deserialize if it is necessary. 

$job_id is optional argument.

=over 4

=item * $job_id (optional)

lookup specific $job_id's job.

=item * $options->{enqueue_time_callback}

specific enqueue_time creation callback.

Default local time create.

=back

=head2 $jonk->errstr;

get most recent error infomation.

=head1 ERROR HANDLING

  my $job = $jonk->lookup;
  if ($jonk->errstr) {
      die $jonk->errstr;
  }

=head1 SCHEMA

=head2 MySQL

    CREATE TABLE job (
        id            int(10) unsigned NOT NULL auto_increment,
        func          varchar(255)     NOT NULL,
        arg           MEDIUMBLOB,
        enqueue_time  DATETIME         NOT NULL,
        grabbed_until int(10) UNSIGNED NOT NULL,
        primary key ( id )
    ) ENGINE=InnoDB

=head2 SQLite

    CREATE TABLE job (
        id            INTEGER PRIMARY KEY ,
        func          text,
        arg           text,
        enqueue_time  text,
        grabbed_until INTEGER UNSIGNED
    )

=head2 PostgreSQL

    CREATE TABLE job (
        id            SERIAL PRIMARY KEY,
        func          TEXT NOT NULL,
        arg           BYTEA,
        enqueue_time  TIMESTAMP NOT NULL,
        grabbed_until INTEGER NOT NULL
    )

=head1 SUPPORT

  irc: #jonk@irc.perl.org

=head1 REPOSITORY

  git clone git://github.com/nekokak/p5-Jonk.git

=head1 CONTRIBUTORS

tokuhirom

kan_fushihara

fujiwara

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

