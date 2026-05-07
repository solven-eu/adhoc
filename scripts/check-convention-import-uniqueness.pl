#!/usr/bin/env perl
# Detect simple class names imported from more than one fully-qualified package across the project.
#
# Rule (from CONVENTIONS.MD — "One simple name → one FQCN, project-wide"):
#   Each Java simple class name (Date, Field, Builder, …) must resolve to a SINGLE fully-qualified
#   class name across the whole project. When two distinct types share a simple name (e.g.
#   java.sql.Date vs java.util.Date), import only ONE; the other must always be written
#   fully-qualified inline.
#
# Waiver: `scripts/import-uniqueness.allow` pins the dominant FQCN for known conflicts. When a
# simple name is pinned there, this checker only reports non-dominant imports (the actionable
# cleanup) and stays silent about correct usages of the dominant FQCN. Simple names absent from
# the waiver still emit the full multi-FQCN report.
#
# Usage:  perl scripts/check-convention-import-uniqueness.pl [file|dir ...]
#         Directories are searched recursively for *.java.
#         Skips: target/, .claude/, worktrees/, generated-sources, build/.
# Exit:   0 = clean OR informational (no CI failure today; CONVENTIONS.MD: "no failure on
#         convention issues"). 1 reserved for future strict mode.

use strict;
use warnings;
use File::Basename qw(dirname);
use File::Find;
use File::Spec;

# ── Load waiver file ────────────────────────────────────────────────────
my $script_dir = dirname(__FILE__);
my $waiver_path = File::Spec->catfile($script_dir, 'import-uniqueness.allow');
my %dominant;    # SimpleName -> dominant.FQCN
if (-f $waiver_path) {
    open my $wfh, '<', $waiver_path or die "Cannot open $waiver_path: $!";
    while (my $line = <$wfh>) {
        chomp $line;
        $line =~ s/#.*$//;        # strip comments
        $line =~ s/^\s+|\s+$//g;  # trim
        next unless length $line;
        my ($simple, $fqcn) = split /\s+/, $line, 2;
        next unless defined $simple && defined $fqcn;
        $dominant{$simple} = $fqcn;
    }
    close $wfh;
}

# ── Walk the tree and collect imports ──────────────────────────────────
my @files = @ARGV ? @ARGV : ('.');
my @java;
for my $arg (@files) {
    if (-f $arg) { push @java, $arg }
    else {
        find(
            sub {
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
        next if $line =~ /^\s*import\s+static\s+/;
        next unless $line =~ /^\s*import\s+([a-zA-Z_][\w.]*)\s*;\s*$/;
        my $fqcn = $1;
        next if $fqcn =~ /\.\*$/;
        my ($simple) = $fqcn =~ /([^.]+)$/;
        next unless defined $simple;
        push @{ $byName{$simple}{$fqcn} }, $file;
    }
    close $fh;
}

# ── Report ─────────────────────────────────────────────────────────────
my $waived_violations = 0;
my $unwaived_conflicts = 0;

for my $simple (sort keys %byName) {
    my @fqcns = sort keys %{ $byName{$simple} };

    if (exists $dominant{$simple}) {
        # Special FQCN `*` means "intentionally polysemous, accept any number of FQCNs".
        next if $dominant{$simple} eq '*';

        # Waived: only flag the non-dominant imports.
        my $dom = $dominant{$simple};
        my @bad = grep { $_ ne $dom } @fqcns;
        next unless @bad;

        my $dom_present = exists $byName{$simple}{$dom};
        my $dom_count = $dom_present ? scalar @{ $byName{$simple}{$dom} } : 0;
        print "Simple name `$simple` — non-dominant imports (dominant: $dom"
            . ($dom_present ? ", in $dom_count file" . ($dom_count == 1 ? '' : 's') : ", not present in repo")
            . "):\n";
        for my $fqcn (@bad) {
            my @uses = @{ $byName{$simple}{$fqcn} };
            my $count = scalar @uses;
            print "  - $fqcn   (in $count file" . ($count == 1 ? '' : 's') . ")\n";
            my $shown = 0;
            for my $f (@uses) {
                print "      $f\n";
                last if ++$shown >= 5;
            }
            if ($count > 5) {
                print "      … (" . ($count - 5) . " more)\n";
            }
            $waived_violations += $count;
        }
        print "\n";
        next;
    }

    next if @fqcns < 2;
    $unwaived_conflicts++;
    print "Simple name `$simple` is imported from "
        . scalar(@fqcns)
        . " different FQCNs (no canonical pick declared in scripts/import-uniqueness.allow):\n";
    for my $fqcn (@fqcns) {
        my @uses = @{ $byName{$simple}{$fqcn} };
        my $count = scalar @uses;
        print "  - $fqcn   (in $count file" . ($count == 1 ? '' : 's') . ")\n";
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

if ($waived_violations == 0 && $unwaived_conflicts == 0) {
    print "No simple-name conflicts (waived rules respected, no unpinned conflicts).\n";
    exit 0;
}
print "Waived violations: $waived_violations file-import" . ($waived_violations == 1 ? '' : 's')
    . " (non-dominant FQCN; remove the import and fully-qualify inline).\n";
print "Unwaived conflicts: $unwaived_conflicts simple name"
    . ($unwaived_conflicts == 1 ? '' : 's')
    . " with no canonical pick yet (declare in scripts/import-uniqueness.allow).\n";
# Informational only — no CI failure today (per project policy on convention checks).
exit 0;
