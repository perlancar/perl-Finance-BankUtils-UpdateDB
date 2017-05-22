package Finance::BankUtils::UpdateDB;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;

our %SPEC;

use Exporter qw(import);
our @EXPORT_OK = qw(update_banktx_db);

my $sch_identifier = ['str*', match=>qr/\A\w+\z/];

$SPEC{update_banktx_db} = {
    v => 1.1,
    summary => '',
    description => <<'_',

This routine is useful if you are storing bank transactions in a database and
want to update the database with new transactions retrieved from the bank's
internet banking site. This routine connects to database, retrieves currently
posted transactions, and also accepts list of updated transactions (e.g. parsed
from internet banking website) and performs necessary updates to the database to
reflect the updated transactions.

Why is this routine needed? Ideally, bank should never updates/deletes already
posted transactions or insert new transactions in the middle. In other words,
bank should always add new transactions at the end. For example, at time T1:

    # date     | seq | description   | amount
    2017-05-22 | 1   | Transaction 1 | $15.23
    2017-05-22 | 2   | Transaction 2 | -$2.80
    2017-05-22 | 3   | Transaction 3 | $27.75

At a later time T2:

    # date     | seq | description                      | amount
    2017-05-22 | 1   | Transaction 1                    | $15.23
    2017-05-22 | 2   | Transaction 2                    | -$2.80
    2017-05-22 | 3   | Transaction 4 Clearing           | $67.05
    2017-05-22 | 4   | Reverse correction transaction 2 |  $2.80

But in the real world, the bank sometimes deletes previously posted transactions
or inserts transactions in the middle, e.g. at time T2 instead of the above we
might see:

    # date     | seq | description            | amount
    2017-05-22 | 1   | Transaction 4 Clearing | $67.05
    2017-05-22 | 2   | Transaction 1          | $15.23
    2017-05-22 | 3   | Transaction 3          | $27.75

That is, Transaction 2 disappears entirely and Transaction 4 is inserted at the
beginning of the day even though it happens after Transactions 1, 2, and 3. This
means, we cannot simply do serial inserts to the database but must do a diff
with previously existing transactions in the database.

This routine accepts arguments to connect to database. You can either provide
`dbh` directly or `dbh_dsn` + `dbh_username` + `dbh_password` (useful when using
this routine on CLI). This routine will then select existing transactions on the
database, on a date-by-date/day-by-day basis. You may need to specify extra
filters using `db_select_filter`, for example if you put transactions from
several bank accounts into the same table. You might also need to specify the
name of your `date`, `seq`, `description`, and `amount` columns. And lastly, you
might also want to specify `db_select_protected_field` to define which
transaction should be protected from deletion.

This routine will then compare the transactions in the database with
`update_txs` which you specify, on a date-by-date/day-by-day basis. `update_txs`
is an array of transactions, where each transaction is a hash that must contain
at least `date`, `description` and `amount`. The routine will try to perform SQL
statements to bring the database up to date. If no update is needed, the routine
will return 304 status (Not Modified). If succeeds, the routine will return 200
status. Otherwise it will return 500 status with the error message.

Using the above example, `updated_txs` is:

    [
      {description=>'Transaction 4 Clearing', amount=>'67.05'},
      {description=>'Transaction 1', amount=>'15.23'},
      {description=>'Transaction 3', amount=>'27.75'},
    ]

This routine will execute SQL statements like these:

    BEGIN WORK;

    -- the actual table name and `id` and `seq` column names are configurable
    UPDATE tablename SET seq=4 WHERE id=1235;

    UPDATE tablename SET seq=3 WHERE id=1234;

    UPDATE tablename SET seq=2 WHERE id=1233;

    # extra column to be set can be specified
    INSERT INTO tablename (seq,description,amount, date,acc_id) VALUES (1,'Transaction 4 Clearing','67.05', '2017-05-22','8');

    DELETE FROM tablename WHERE id=1234;

    UPDATE tablename SET seq=3 WHERE id=1235';

    UPDATE tablename SET seq=2 WHERE id=1234;

    COMMIT;

If there is a protected transaction that is attempted to be removed, the routine
will fail with 412 status, e.g.:

    [412, "Can't remove protected transaction (id=1234)"]

_
    args => {
        updated_txs => {
            schema => ['array*', of=>[
                'hash*', {
                    'keys.restrict'=>0,
                    req_keys=>[qw/description amount/],
                    keys=>{
                        date=>'str*',
                        description=>'str*',
                        amount=>'str*',
                    },
                }]],
            req => 1,
        },

        dbh => {
            schema => 'obj*',
            tags => ['category:database', 'hidden-cli'],
        },
        db_dsn => {
            schema => 'str*',
            tags => ['category:database'],
        },
        db_username => {
            schema => 'str*',
            tags => ['category:database'],
        },
        db_password => {
            schema => 'str*',
            tags => ['category:database'],
        },

        db_table => {
            schema => $sch_identifier,
            tags => ['category:database'],
            req => 1,
        },
        db_insert_extra_columns => {
            schema => ['hash*', each_key=>qr/\A\w+\z/],
            tags => ['category:database'],
            description => <<'_',

If your ID column is not a sequence/"auto increment", you'll need to provide it
youself here.

_
        },
        db_select_filter => {
            summary => 'More WHERE clause to filter transactions',
            schema => 'str*',
        },
        db_select_protected_field => {
            summary => 'What term to determine whether a transaction should be protected from deletion',
            description => <<'_',

Example:

    -- transaction has been used/assigned an invoice
    'EXISTS(SELECT 1 FROM banktx_inv WHERE banktx_id=banktx.id)'

_
            schema => 'str*',
        },
        db_seq_column => {
            schema => $sch_identifier,
            default => 'seq',
            tags => ['category:sql'],
        },
        db_id_column => {
            schema => $sch_identifier,
            default => 'id',
            tags => ['category:database'],
        },
    },
    args_rels => {
        req_one => [qw/dbh db_dsn/],
        choose_all => [qw/db_dsn db_username db_password/],
    },
    features => {
        dry_run => 1,
    },
};
sub update_banktx_db {
    require Algorithm::Diff;

    # check input

    my %args = @_;
    my $db_table = $args{db_table} or return [400, "Please specify db_table"];
    my $db_seq_column = $args{db_seq_column} // "seq";
    my $db_id_column  = $args{db_id_column}  // "id";
    my $db_insert_extra_columns  = $args{db_insert_extra_columns}  // {};

    my $db_txs = $args{db_txs} or return [400, "Please specify db_txs"];
    ref($db_txs) eq 'ARRAY' or return [400, "db_txs must be an array"];
    my $i = 0;
    my %seen_ids;
    for my $tx (@$db_txs) {
        ref($tx) eq 'HASH' or return [400, "db_txs[$i] must be a hash"];
        $tx->{seq} or return [400, "db_txs[$i]: seq is zero or undefined"];
        $tx->{seq} == $i+1 or return [400, "db_txs[$i]: seq must be ".($i+1)];
        defined($tx->{description}) or return [400, "db_txs[$i]: description must be defined"];
        defined($tx->{amount}) or return [400, "db_txs[$i]: amount must be defined"];
        defined($tx->{id}) or return [400, "db_txs[$i]: id must be defined"];
        $seen_ids{$tx->{id}}++ and return [400, "db_txs[$i]: id is not unique"];
    }
    my $updated_txs = $args{updated_txs} or return [400, "Please specify updated_txs"];
    ref($updated_txs) eq 'ARRAY' or return [400, "updated_txs must be an array"];
    $i = 0;
    for my $tx (@$updated_txs) {
        ref($tx) eq 'HASH' or return [400, "updated_txs[$i] must be a hash"];
        defined($tx->{description}) or return [400, "updated_txs[$i]: description must be defined"];
        defined($tx->{amount}) or return [400, "updated_txs[$i]: amount must be defined"];
    }

    my $is_modified;

    # diff
    {
        # diff() refers to positions of the original sequence but after each
        # addition and deletion, the index will change. this array maintains the
        # mapping of old positions to new.
        my @new_pos = 0..$#{$db_txs};

        my @hunks = Algorithm::Diff::diff(
            $db_txs, $updated_txs, sub { "$_[0]{description}|$_[0]{amount}" });

        # check first if diff attempts to remove

        for my $hunk (@hunks) {
            for my $item (@$hunk) {
                my ($sign, $pos, $tx) = @$item;
                if ($sign eq '+') {
                    for my $i (reverse $pos .. $#{$db_txs}) {
                        my $tx = $db_txs->[$i];
                        $new_pos[$i]++;
                        $tx->{seq}++;
                        push @sql, "UPDATE $sql_table SET $sql_seq_column=$tx->{seq}".
                            " WHERE $sql_id_column=".$code_quote->($tx->{id});
                    }
                    my $seq = $new_pos[$pos]+1;
                    my $new_tx = {
                        description => $tx->{description},
                        amount => $tx->{amount},
                        seq => $seq,
                    };
                    $new_tx->{$_} //= $sql_insert_extra_columns->{$_} for
                        keys %$sql_insert_extra_columns;

                    splice @$db_txs, $seq-1, 0, $new_tx;

                    my %cols;
                    $cols{description} = $tx->{description};
                    $cols{amount} = $tx->{amount};
                    $cols{$sql_seq_column} = $new_p
                    $cols{$_} //= $sql_insert_extra_columns->{$_} for
                        keys %$sql_insert_extra_columns;
                    my @cols = sort keys %cols;
                    push @sql, "INSERT INTO $sql_table (".join(",", @cols).
                        ") VALUES (".join(",", map {$code_quote->($cols{$_})} @cols).")";
                } elsif ($sign eq '-') {
                    for my $i (reverse $pos+1 .. $#{$db_txs}) {
                        my $tx = $db_txs->[$i];
                        $new_pos[$i]--;
                        $tx->{seq}++;
                        push @sql, "UPDATE $sql_table SET $sql_seq_column=$tx->{seq}".
                            " WHERE $sql_id_column=".$code_quote->($tx->{id});
                    }
                } else {
                    # should never happen
                    return [500, "Bug: unknown hunk sign '$sign', it should only be either + or -"];
                }
            }
        }
    }

    [200, "OK", \@sql];
}

1;
# ABSTRACT: Utilities for updating database of bank transactions

=head1 DESCRIPTION

=head1 SEE ALSO

=cut
