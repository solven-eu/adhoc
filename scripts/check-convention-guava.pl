#!/usr/bin/env perl
# Detect JDK-collection usage where Guava immutable collections are preferred.
# Rule (from CONVENTIONS.MD — "Immutability first"):
#   - `Set.of(`      → prefer ImmutableSet.of(
#   - `List.of(`     → prefer ImmutableList.of(
#   - `Map.of(`      → prefer ImmutableMap.of(
#   - `Collectors.toSet()`  → prefer ImmutableSet.toImmutableSet()
#   - `Collectors.toList()` → prefer ImmutableList.toImmutableList()
#   - `new ArrayList<`      → prefer ImmutableList.of / ImmutableList.copyOf (when immutable)
#
# Usage:  perl scripts/check-convention-guava.pl [file|dir ...]
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

        # JDK factory methods — prefer Guava equivalents
        if ($code =~ /(?<![A-Za-z0-9_])Set\s*\.\s*of\s*\(/) {
            print "$file:$lineno: 'Set.of(' — prefer ImmutableSet.of(\n";
            $total++;
        }
        if ($code =~ /(?<![A-Za-z0-9_])List\s*\.\s*of\s*\(/) {
            print "$file:$lineno: 'List.of(' — prefer ImmutableList.of(\n";
            $total++;
        }
        if ($code =~ /(?<![A-Za-z0-9_])Map\s*\.\s*of\s*\(/) {
            print "$file:$lineno: 'Map.of(' — prefer ImmutableMap.of(\n";
            $total++;
        }
        # Collectors streaming to mutable JDK collections
        if ($code =~ /Collectors\s*\.\s*toSet\s*\(\s*\)/) {
            print "$file:$lineno: 'Collectors.toSet()' — prefer ImmutableSet.toImmutableSet()\n";
            $total++;
        }
        if ($code =~ /Collectors\s*\.\s*toList\s*\(\s*\)/) {
            print "$file:$lineno: 'Collectors.toList()' — prefer ImmutableList.toImmutableList() or .toList()\n";
            $total++;
        }
    }
    close $fh;
}

print "\n$total Guava convention violation(s) found.\n" if $total;
exit($total ? 1 : 0);
