#! /usr/bin/perl
use strict;
use warnings;
use Benchmark qw/countit timethese timeit timestr/;
use Jonk;
use DBI;
use Parallel::ForkManager;

my $db = DBI->connect('dbi:mysql:test','root','');
$db->do(q{DROP TABLE job});
$db->do(q{
    CREATE TABLE job (
        id           int(10) unsigned NOT NULL auto_increment,
        func         varchar(255)     NOT NULL,
        arg          MEDIUMBLOB,
        enqueue_time DATETIME         NOT NULL,
        primary key ( id )
    )
});
my $pm = Parallel::ForkManager->new(50);

timethese(5, {
    'enqueue' => sub {
        for my $i (1 .. 1000) {
            my $pid = $pm->start and next;

                my $dbh = DBI->connect('dbi:mysql:test','root','');
                my $jonk = Jonk->new($dbh);
                my $job_id = $jonk->enqueue('MyWorker','args_'.$i);

            $pm->finish;
        }
        $pm->wait_all_children;
    },
});

timethese(5, {
    'dequeue' => sub {
        for my $i (1 .. 1000) {
            my $pid = $pm->start and next;

            my $dbh = DBI->connect('dbi:mysql:test','root','');
            my $jonk = Jonk->new($dbh,{funcs => [qw/MyWorker/]});
            my $job = $jonk->dequeue;

            $pm->finish;
        }
        $pm->wait_all_children;
    },
});

__END__
Benchmark: timing 5 iterations of enqueue...
   enqueue: 24 wallclock secs ( 0.67 usr  2.67 sys + 11.93 cusr 21.04 csys = 36.31 CPU) @  0.14/s (n=5)
Benchmark: timing 5 iterations of dequeue...
   dequeue: 55 wallclock secs ( 0.74 usr  3.70 sys + 12.73 cusr 28.16 csys = 45.33 CPU) @  0.11/s (n=5)

