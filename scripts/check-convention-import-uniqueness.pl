#!/usr/bin/env perl
# Detect simple class names imported from more than one fully-qualified package across the project.
#
# Rule (from CONVENTIONS.MD — "One simple name → one FQCN, project-wide"):
#   Each Java simple class name (Date, Field, Builder, …) must resolve to a SINGLE fully-qualified
#   class name across the whole project. When two distinct types share a simple name (e.g.
#   java.sql.Date vs java.util.Date), import only ONE; the other must always be written
#   fully-qualified inline.
#
# Usage:  perl scripts/check-convention-import-uniqueness.pl [file|dir ...]
#         Directories are searched recursively for *.java.
#         Skips: target/, .claude/, worktrees/, generated-sources, build/.
# Exit:   0 = clean, 1 = violations found.

use strict;
use warnings;
use File::Find;

my @files = @ARGV ? @ARGV : ('.');
my @java;
for my $arg (@files) {
    if (-f $arg) { push @java, $arg }
    else {
        find(
            sub {
                # Prune common output / vendor directories
                if (-d $_ && /^(target|build|generated-sources|node_modules|\.git)$/) {
                    $File::Find::prune = 1;
                    return;
                }
                if (-d $_ && $File::Find::name =~ m{/\.claude(/|$)}) {
                    $File::Find::prune = 1;
                    return;
                }
                if (-d $_ && /^worktrees$/) {
                    $File::Find::prune = 1;
                    return;
                }
                push @java, $File::Find::name if /\.java$/ && -f $_;
            },
            $arg
        );
    }
}

# simpleName -> { fqcn -> [files] }
my %byName;

for my $file (sort @java) {
    open my $fh, '<', $file or do {
        warn "Cannot open $file: $!";
        next;
    };
    while (my $line = <$fh>) {
        # Skip `import static …` (we care about type imports only).
        next if $line =~ /^\s*import\s+static\s+/;
        # Capture `import some.qualified.Path.SimpleName;` — non-wildcard, non-static.
        next unless $line =~ /^\s*import\s+([a-zA-Z_][\w.]*)\s*;\s*$/;
        my $fqcn = $1;
        next if $fqcn =~ /\.\*$/;    # wildcard imports — skip
        my ($simple) = $fqcn =~ /([^.]+)$/;
        next unless defined $simple;
        push @{ $byName{$simple}{$fqcn} }, $file;
    }
    close $fh;
}

my $total = 0;
my @conflicts;
for my $simple (sort keys %byName) {
    my @fqcns = sort keys %{ $byName{$simple} };
    next if @fqcns < 2;

    push @conflicts, $simple;
    $total++;
    print "Simple name `$simple` is imported from "
        . scalar(@fqcns)
        . " different FQCNs:\n";
    for my $fqcn (@fqcns) {
        my @uses = @{ $byName{$simple}{$fqcn} };
        my $count = scalar @uses;
        print "  - $fqcn   (in $count file" . ($count == 1 ? '' : 's') . ")\n";
        # Print up to 5 sample files so the report stays readable.
        my $shown = 0;
        for my $f (@uses) {
            print "      $f\n";
            last if ++$shown >= 5;
        }
        if ($count > 5) {
            print "      … (" . ($count - 5) . " more)\n";
        }
    }
    print "\n";
}

if ($total == 0) {
    print "No simple-name conflicts across imports.\n";
    exit 0;
}
print "Total simple-name conflicts: $total\n";
exit 1;
