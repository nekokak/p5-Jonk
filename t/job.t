use t::Utils;
use Test::More;
use Jonk;

my $dbh = t::Utils->setup;

subtest 'failed job' => sub {
    my $client = Jonk->new($dbh, {functions => [qw/MyWorker/]});

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    {
        my $job = $client->lookup_job($job_id);
        ok $job;
        is $job->arg, 'arg';
        is $job->func, 'MyWorker';
        is $job->retry_cnt, 0;
        is $job->run_after, 0;
        is $job->priority, 0;

        ok not $client->errstr;

        ok not $job->is_completed;
        ok not $job->is_failed;
        ok not $job->is_aborted;

        $job->failed({retry_delay => 2});

        ok not $job->is_completed;
        ok     $job->is_failed;
        ok not $job->is_aborted;

        ok not $client->lookup_job($job_id);
    }

    {
        sleep 2;

        my $job = $client->lookup_job($job_id);

        ok $job;
        is $job->arg, 'arg';
        is $job->func, 'MyWorker';
        is $job->retry_cnt, 1;
        ok $job->run_after > 0;
        is $job->priority, 0;

        ok not $job->is_completed;
        ok not $job->is_failed;
        ok not $job->is_aborted;

        $job->aborted();

        ok not $job->is_completed;
        ok not $job->is_failed;
        ok     $job->is_aborted;

        ok not $client->lookup_job($job_id);

        my $sth = $dbh->prepare('SELECT * FROM job WHERE id = ?');
        $sth->execute($job_id);
        ok not $sth->fetchrow_hashref;
    }

    {
        my $job_id = $client->insert('MyWorker', 'arg');
        ok $job_id;

        my $job = $client->lookup_job($job_id);
        ok not $job->is_completed;
        ok not $job->is_failed;
        ok not $job->is_aborted;

        $job->completed();

        ok     $job->is_completed;
        ok not $job->is_failed;
        ok not $job->is_aborted;

        ok not $client->lookup_job($job_id);

        my $sth = $dbh->prepare('SELECT * FROM job WHERE id = ?');
        $sth->execute($job_id);
        ok not $sth->fetchrow_hashref;
    }

    subtest 'failed and set priority' => sub {
        my $job_id = $client->insert('MyWorker', 'arg');
        ok $job_id;

        my $job = $client->lookup_job($job_id);
        is $job->priority, 0;
        $job->failed({retry_delay => 0});

        $job = $client->lookup_job($job_id);
        is $job->priority, 0;
        $job->failed({retry_delay => 0, priority => 1});

        $job = $client->lookup_job($job_id);
        is $job->priority, 1;

        $job->completed; 
    };
};

subtest 'error case' => sub {
    my $client = Jonk->new($dbh, {functions => [qw/MyWorker/]});

    {
        my $job_id = $client->insert('MyWorker', 'arg');
        ok $job_id;

        my $job = $client->lookup_job($job_id);

        $job->completed();

        eval {$job->failed};
        like $@ , qr/job is already completed./;
        eval {$job->aborted};
        like $@ , qr/job is already completed./;
    }
    {
        my $job_id = $client->insert('MyWorker', 'arg');
        ok $job_id;

        my $job = $client->lookup_job($job_id);

        $job->failed();

        eval {$job->completed};
        like $@ , qr/job is already failed./;
        eval {$job->aborted};
        like $@ , qr/job is already failed./;
    }
    {
        my $job_id = $client->insert('MyWorker', 'arg');
        ok $job_id;

        my $job = $client->lookup_job($job_id);

        $job->aborted();

        eval {$job->completed};
        like $@ , qr/job is already aborted./;
        eval {$job->failed};
        like $@ , qr/job is already aborted./;
    }
};

done_testing;

