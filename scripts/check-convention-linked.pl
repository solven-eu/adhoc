#!/usr/bin/env perl
# Detect unordered-collection violations in Java source files.
# Rule (from CONVENTIONS.MD — "Ordered mutability when mutation is needed"):
#   - `new HashMap`        → prefer LinkedHashMap
#   - `new HashSet`        → prefer LinkedHashSet
#   - `Collectors.groupingBy(fn, downstream)` 2-arg form → use 3-arg with LinkedHashMap::new
#
# Usage:  perl scripts/check-convention-linked.pl [file|dir ...]
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
    while (my $line = <$fh>) {
        my $lineno = $.;
        my $code = $line =~ s|//.*||r;    # strip single-line comments

        if ($code =~ /\bnew\s+HashMap\s*[(<]/) {
            print "$file:$lineno: 'new HashMap' — if mutable: use LinkedHashMap; if immutable: use ImmutableMap\n";
            $total++;
        }
        if ($code =~ /\bnew\s+HashSet\s*[(<]/) {
            print "$file:$lineno: 'new HashSet' — if mutable: use LinkedHashSet; if immutable: use ImmutableSet\n";
            $total++;
        }
        # Collectors.toMap without LinkedHashMap: 2-arg or 3-arg (merge fn) but no map factory.
        # Heuristic: line contains toMap( but not LinkedHashMap and not PepperStreamHelperHacked.
        if ($code =~ /Collectors\s*\.\s*toMap\s*\(/ &&
                $code !~ /LinkedHashMap/ &&
                $code !~ /PepperStreamHelperHacked/ &&
                $code !~ /toLinkedMap/) {
            print "$file:$lineno: 'Collectors.toMap' without LinkedHashMap — prefer PepperStreamHelperHacked.toLinkedMap or 4-arg Collectors.toMap(..., LinkedHashMap::new)\n";
            $total++;
        }
    }
    close $fh;
}

print "\n$total linked-collection convention violation(s) found.\n" if $total;
exit($total ? 1 : 0);
