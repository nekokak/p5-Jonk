#! perl
use t::Utils;
use JSON::XS;
use DBI;

$SIG{INT} = sub { CORE::exit 1 };
$mysqld = t::Utils->setup;
$ENV{TEST_MYSQLD} = encode_json +{ %$mysqld };

my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'));
$dbh->do(
q{
CREATE TABLE job (
    id           int(10) unsigned NOT NULL auto_increment,
    func         varchar(255)     NOT NULL,
    arg          MEDIUMBLOB,
    enqueue_time DATETIME         NOT NULL,
    primary key ( id )
)
}
);

