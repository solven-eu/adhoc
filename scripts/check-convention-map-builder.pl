#!/usr/bin/env perl
# Detect Map.of() / ImmutableMap.of() calls with 2 or more entries.
# Rule (from CONVENTIONS.MD — "Map builder readability"):
#   Map.of( and ImmutableMap.of( are allowed only for 0 or 1 entry.
#   For 2+ entries, prefer ImmutableMap.builder().put(...).put(...).build()
#   for readability.
#
# Note: comma counting is done at the syntax level (paren-depth tracking).
# String literals that contain commas can cause false positives, but are rare.
#
# Usage:  perl scripts/check-convention-map-builder.pl [file|dir ...]
#         Directories are searched recursively for *.java.
# Exit:   0 = clean, 1 = violations found.

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
    my @lines = <$fh>;
    close $fh;

    my $content = join('', @lines);

    # Locate every Map.of( / ImmutableMap.of( call
    while ($content =~ /(?<![A-Za-z0-9_])((?:Immutable)?Map\s*\.\s*of)\s*\(/g) {
        my $call_name   = $1;            # "Map.of" or "ImmutableMap.of"
        my $open_paren  = pos($content) - 1;   # index of '('

        # Determine 1-based line number of the opening paren
        my $before  = substr($content, 0, pos($content) - length($call_name) - 1);
        my $lineno  = () = $before =~ /\n/g;
        $lineno++;

        # Walk the argument list, counting top-level commas
        my $depth   = 1;
        my $commas  = 0;
        my $i       = $open_paren + 1;
        my $len     = length($content);
        while ($i < $len && $depth > 0) {
            my $ch = substr($content, $i, 1);
            if    ($ch eq '(') { $depth++ }
            elsif ($ch eq ')') { $depth-- }
            elsif ($ch eq ',' && $depth == 1) { $commas++ }
            $i++;
        }

        # Each entry is a (key, value) pair = 2 args.
        # 1 entry  → 1 comma  (allowed)
        # 2 entries → 3 commas (violation)
        # Threshold: commas >= 3  →  2+ entries
        if ($commas >= 3) {
            my $entries = int(($commas + 1) / 2);
            print "$file:$lineno: '$call_name(' with $entries entries"
                . " — prefer ImmutableMap.builder().put(\342\200\246).build()\n";
            $total++;
        }
    }
}

print "\n$total map-builder convention violation(s) found.\n" if $total;
exit($total ? 1 : 0);
