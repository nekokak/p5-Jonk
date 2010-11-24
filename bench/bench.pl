#! /usr/bin/perl
use strict;
use warnings;
use Benchmark qw/countit timethese timeit timestr/;
use Jonk::Client;
use Jonk::Worker;
use DBI;
use Parallel::ForkManager;

my $db = DBI->connect('dbi:mysql:jonk','root','');
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

my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
my $time = sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);

timethese(10, {
    'enqueue' => sub {
        for my $i (1 .. 10000) {
            my $pid = $pm->start and next;

                my $dbh = DBI->connect('dbi:mysql:jonk','root','');
                my $jonk = Jonk::Client->new($dbh);
                my $job_id = $jonk->enqueue('MyWorker3',+{arg => 'args_'.$i, time => $time});

            $pm->finish;
        }
        $pm->wait_all_children;
    },
});

timethese(10, {
    'dequeue' => sub {
        for my $i (1 .. 10000) {
            my $pid = $pm->start and next;

            my $dbh = DBI->connect('dbi:mysql:jonk','root','');
            my $jonk = Jonk::Worker->new($dbh,{functions => [qw/MyWorker2/]});
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

