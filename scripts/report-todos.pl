#!/usr/bin/env perl
# Report all TODO comments found in Java source files.
#
# This script is purely informational: it always exits 0.
# Its output lets the AI quality agent (and human reviewers) see every
# outstanding TODO so they can decide which ones to address.
#
# Output format (one line per hit):
#   <file>:<lineno>: <TODO comment text>
#
# Usage:  perl scripts/report-todos.pl [file|dir ...]
#         Directories are searched recursively for *.java.

use strict;
use warnings;
use File::Find;

my @files = @ARGV ? @ARGV : ('.');
my @java;
for my $arg (@files) {
    if (-f $arg) { push @java, $arg }
    else { find(sub { push @java, $File::Find::name if /\.java$/ }, $arg) }
}

my $total = 0;
for my $file (sort @java) {
    open my $fh, '<', $file or die "Cannot open $file: $!";
    while (my $line = <$fh>) {
        my $lineno = $.;
        # Match // TODO ... (anywhere in the line, including after code)
        if ($line =~ m{//\s*(TODO\b.*?)\s*$}i) {
            my $comment = $1;
            print "$file:$lineno: $comment\n";
            $total++;
        }
    }
    close $fh;
}

print "\n$total TODO(s) found.\n";
exit 0;
