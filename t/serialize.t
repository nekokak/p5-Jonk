use t::Utils;
use Test::More;
use DBI;
use Jonk;
use Storable ();

my $dbh = t::Utils->setup;

subtest '(de)serializer set each functions' => sub {
    my $serialized_arg;
    my $jonk = Jonk->new($dbh, {
        functions => [
            'MyWorker' => {
                serializer   => sub {$serialized_arg = Storable::nfreeze($_[0])},
                deserializer => sub {Storable::thaw($_[0])}
            }
        ]
    });

    subtest 'job completed' => sub {
        my $job_id = $jonk->insert('MyWorker', {chars => 'bar'});
        ok $job_id;

        my $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->completed;

        ok not $jonk->errstr;
    };

    subtest 'job failed and retry' => sub {

        my $job_id = $jonk->insert('MyWorker', {chars => 'bar'});
        ok $job_id;

        my $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->failed({retry_delay => 0});

        $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->completed;

        ok not $jonk->errstr;
    };
};

subtest '(de)serializer set global' => sub {
    my $serialized_arg;
    my $jonk = Jonk->new($dbh, {
        functions => [qw/MyWorker/],
        default_serializer   => sub {$serialized_arg = Storable::nfreeze($_[0])},
        default_deserializer => sub {Storable::thaw($_[0])},
    });

    subtest 'job completed' => sub {
        my $job_id = $jonk->insert('MyWorker', {chars => 'bar'});
        ok $job_id;

        my $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->completed;

        ok not $jonk->errstr;
    };

    subtest 'job failed and retry' => sub {

        my $job_id = $jonk->insert('MyWorker', {chars => 'bar'});
        ok $job_id;

        my $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->failed({retry_delay => 0});

        $job = $jonk->lookup_job($job_id);
        is $job->id, $job_id;
        is_deeply $job->arg, {chars => 'bar'};
        is $job->raw_arg, $serialized_arg;

        $job->completed;

        ok not $jonk->errstr;
    };
};

done_testing;

