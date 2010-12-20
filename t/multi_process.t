use strict;
use warnings;
use t::Utils;
use Test::More;
use Jonk;

my $dbh = t::Utils->setup;

subtest 'dequeue' => sub {

    {
        my $client = Jonk->new($dbh);

        $client->enqueue('MyWorker', 'arg1');
        $client->enqueue('MyWorker', 'arg2');
    }

    if ( fork ) {
        my $dbh = t::Utils->setup;

        my $jonk = Jonk->new($dbh, {functions => [qw/MyWorker/]});
        my $job = $jonk->lookup();
        is $job->arg, 'arg1';

        wait;
    }
    else {
        my $dbh = t::Utils->setup;

        sleep 1;

        my $jonk = Jonk->new($dbh, {functions => [qw/MyWorker/]});
        my $job = $jonk->lookup();

        is $job->arg, 'arg2';
    }
};

done_testing;

