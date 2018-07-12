#! /usr/bin/env perl

use strict;
use warnings;

unless (@ARGV >= 2) {
    print "$0 DATABASE COLUMN [SITE ...]\n";
    exit 1;
}

my $db = shift @ARGV;
my $column = shift @ARGV;

my @sites = @ARGV;

# Remaining arguments are actually sites to ignore if ignore env variable is set.
@sites = grep {chomp; ! ($_ ~~ @sites)} `echo 'SELECT site FROM stats;' | sqlite3 $db` if ($ENV{ignore} or not @sites);

my $differential = 0;
my @maxsite = ("", 0);

for (@sites) {
    chomp;
    my @colcontents = `echo 'SELECT $column FROM stats_history WHERE site = "$_" AND STRFTIME("%Y", entered) = "2018" AND files != 0;' | sqlite3 $db`;
    my $prev = 0;
    my $site_diff = 0;
    for (@colcontents) {
        # Add to the total tracked if the previous value in the column was higher
        $site_diff += ($prev - $_) if ($prev > $_);
        $prev = $_;
    }
    print "$_\n$site_diff\n" if ($ENV{debug});

    @maxsite = ($_, $site_diff) if ($site_diff > $maxsite[1]);
    $differential += $site_diff;
}

print "Maxsite: $maxsite[0] $maxsite[1]\n" if ($ENV{debug});

print "$differential\n";
