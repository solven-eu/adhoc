#!/usr/bin/env perl
# Detect logging convention violations in Java source files.
# Rule (from CONVENTIONS.MD — "Logging"):
#   - log calls must use {} placeholders, never string concatenation.
#   - Wrong:   log.debug("Processing " + count + " rows");
#   - Correct: log.debug("Processing {} rows", count);
#
# Usage:  perl scripts/check-convention-logging.pl [file|dir ...]
#         Directories are searched recursively for *.java (test paths skipped).
# Exit:   0 = clean, 1 = violations found.

use strict;
use warnings;
use File::Find;

my @files = @ARGV ? @ARGV : ('.');
my @java;
for my $arg (@files) {
    if (-f $arg) { push @java, $arg }
    else { find(sub { push @java, $File::Find::name if /\.java$/ && !/\/test\// }, $arg) }
}

my $total = 0;
for my $file (sort @java) {
    open my $fh, '<', $file or die "Cannot open $file: $!";
    while (my $line = <$fh>) {
        my $lineno = $.;
        my $code = $line =~ s|//.*||r;    # strip single-line comments

        # Detect log call where the message argument contains string concatenation (+).
        # Matches: log.debug("..." + ...) or log.warn("prefix " + var + "...")
        if ($code =~ /\blog\s*\.\s*(?:trace|debug|info|warn|error)\s*\(.*"[^"]*"\s*\+/) {
            print "$file:$lineno: log call uses string concatenation — use {} placeholder instead\n";
            $total++;
        }
    }
    close $fh;
}

print "\n$total logging convention violation(s) found.\n" if $total;
exit($total ? 1 : 0);
