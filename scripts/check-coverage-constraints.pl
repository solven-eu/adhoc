#!/usr/bin/env perl
# Compare per-module JaCoCo coverage constraints (from pom.xml properties) with
# the actual bundle-level coverage found in target/site/jacoco/jacoco.xml reports.
#
# For every module that has both a pom.xml and a JaCoCo report, prints a table
# showing the required and actual BRANCH and INSTRUCTION ratios, and whether the
# module passes or fails its own declared constraints.
#
# Exits 1 if any module fails its constraint, 0 otherwise.
#
# Usage:  perl scripts/check-coverage-constraints.pl [root_dir]
#         Default root: current directory.

use strict;
use warnings;
use File::Find;
use File::Basename;

my $root = $ARGV[0] // '.';
$root =~ s{/+$}{};    # strip trailing slashes

# ------------------------------------------------------------------
# 1. Read root pom.xml for default thresholds
# ------------------------------------------------------------------
my $root_pom = "$root/pom.xml";
my %defaults  = ( branch => 0.50, instruction => 0.70 );

if ( -f $root_pom ) {
    my $content = _slurp($root_pom);
    if ( $content =~ /<jacoco\.branch\.ratio>([\d.]+)<\/jacoco\.branch\.ratio>/ ) {
        $defaults{branch} = $1 + 0;
    }
    if ( $content =~ /<jacoco\.instruction\.ratio>([\d.]+)<\/jacoco\.instruction\.ratio>/ ) {
        $defaults{instruction} = $1 + 0;
    }
}

# ------------------------------------------------------------------
# 2. Find all module pom.xml files (skip target/ and .claude/ paths)
# ------------------------------------------------------------------
my @pom_files;
find(
    {
        wanted => sub {
            return if $File::Find::name =~ m{/target/};
            return if $File::Find::name =~ m{/\.claude/};
            return if $File::Find::name eq $root_pom;
            push @pom_files, $File::Find::name
                if basename($File::Find::name) eq 'pom.xml';
        },
        no_chdir => 1,
    },
    $root
);

# ------------------------------------------------------------------
# 3. For each module: read thresholds, locate jacoco.xml, compare
# ------------------------------------------------------------------
my @results;

for my $pom ( sort @pom_files ) {
    my $module_dir = dirname($pom);
    my $content    = _slurp($pom);

    # Extract per-module threshold overrides (fall back to root defaults).
    my $branch_req = $defaults{branch};
    my $instr_req  = $defaults{instruction};
    if ( $content =~ /<jacoco\.branch\.ratio>([\d.]+)<\/jacoco\.branch\.ratio>/ ) {
        $branch_req = $1 + 0;
    }
    if ( $content =~ /<jacoco\.instruction\.ratio>([\d.]+)<\/jacoco\.instruction\.ratio>/ ) {
        $instr_req = $1 + 0;
    }

    # Locate the JaCoCo XML report (aggregate module uses a different sub-dir).
    my $jacoco_xml = "$module_dir/target/site/jacoco/jacoco.xml";
    if ( !-f $jacoco_xml ) {
        $jacoco_xml = "$module_dir/target/site/jacoco-aggregate/jacoco.xml";
    }

    unless ( -f $jacoco_xml ) {
        push @results,
            {
            module     => _rel( $module_dir, $root ),
            branch_req => $branch_req,
            instr_req  => $instr_req,
            branch_act => undef,
            instr_act  => undef,
            status     => 'NO REPORT',
            };
        next;
    }

    # Parse bundle-level counters from jacoco.xml.
    # The JaCoCo XML hierarchy is: report > (group|package) > class > method.
    # Bundle-level <counter> elements are always the last counters in the file —
    # they appear after all </package> (and </group>) closing tags.
    # Scanning the whole file and keeping the last match for each counter type
    # is both correct and robust for single-line XML output produced by JaCoCo.
    my ( $branch_missed, $branch_covered, $instr_missed, $instr_covered ) =
        ( 0, 0, 0, 0 );

    my $xml = _slurp($jacoco_xml);

    while ( $xml =~ /<counter\s+type="BRANCH"\s+missed="(\d+)"\s+covered="(\d+)"/g ) {
        ( $branch_missed, $branch_covered ) = ( $1, $2 );
    }
    while ( $xml =~ /<counter\s+type="INSTRUCTION"\s+missed="(\d+)"\s+covered="(\d+)"/g ) {
        ( $instr_missed, $instr_covered ) = ( $1, $2 );
    }

    my $branch_total = $branch_missed + $branch_covered;
    my $instr_total  = $instr_missed + $instr_covered;

    my $branch_ratio = $branch_total > 0 ? $branch_covered / $branch_total : undef;
    my $instr_ratio  = $instr_total  > 0 ? $instr_covered  / $instr_total  : undef;

    # A counter type with no items (undef ratio) is vacuously passing — JaCoCo
    # itself does not fail a module that has zero branches to cover.
    my $branch_fail = defined $branch_ratio && $branch_ratio < $branch_req;
    my $instr_fail  = defined $instr_ratio  && $instr_ratio  < $instr_req;

    my $status;
    if ( !defined $branch_ratio && !defined $instr_ratio ) {
        $status = 'NO DATA';
    }
    elsif ( $branch_fail || $instr_fail ) {
        $status = 'FAIL';
    }
    else {
        $status = 'PASS';
    }

    push @results,
        {
        module     => _rel( $module_dir, $root ),
        branch_req => $branch_req,
        instr_req  => $instr_req,
        branch_act => $branch_ratio,
        instr_act  => $instr_ratio,
        status     => $status,
        };
}

# ------------------------------------------------------------------
# 4. Print results table
# ------------------------------------------------------------------
my @sorted = sort { $a->{module} cmp $b->{module} } @results;

# Compute dynamic module column width.
my $mod_width = length("Module");
$mod_width = length( $_->{module} ) > $mod_width ? length( $_->{module} ) : $mod_width
    for @sorted;

my $header = sprintf "%-*s  %8s  %9s  %8s  %9s  %s",
    $mod_width, "Module",
    "Br-req", "Br-actual",
    "In-req", "In-actual",
    "Status";
my $sep = '-' x length($header);
print "$header\n$sep\n";

for my $r (@sorted) {
    my $br_act = defined $r->{branch_act} ? sprintf "%.1f%%", $r->{branch_act} * 100 : 'n/a';
    my $in_act = defined $r->{instr_act}  ? sprintf "%.1f%%", $r->{instr_act}  * 100 : 'n/a';

    # Annotate the actual value with '<' when it is below the requirement.
    $br_act = "!$br_act"
        if defined $r->{branch_act} && $r->{branch_act} < $r->{branch_req};
    $in_act = "!$in_act"
        if defined $r->{instr_act} && $r->{instr_act} < $r->{instr_req};

    printf "%-*s  %7.1f%%  %9s  %7.1f%%  %9s  %s\n",
        $mod_width, $r->{module},
        $r->{branch_req} * 100, $br_act,
        $r->{instr_req}  * 100, $in_act,
        $r->{status};
}

# ------------------------------------------------------------------
# 5. Summary
# ------------------------------------------------------------------
my @fails   = grep { $_->{status} eq 'FAIL'      } @results;
my @no_rep  = grep { $_->{status} eq 'NO REPORT' } @results;
my @no_data = grep { $_->{status} eq 'NO DATA'   } @results;
my @passes  = grep { $_->{status} eq 'PASS'      } @results;

printf "\n%d modules: %d PASS, %d FAIL, %d without report, %d without data.\n",
    scalar @results,
    scalar @passes,
    scalar @fails,
    scalar @no_rep,
    scalar @no_data;

if (@fails) {
    print "\nFailing modules:\n";
    for my $r (@fails) {
        my $br_act = sprintf "%.1f%%", $r->{branch_act} * 100;
        my $in_act = sprintf "%.1f%%", $r->{instr_act}  * 100;
        printf "  %-*s  branch %s < required %.1f%%  |  instruction %s < required %.1f%%\n",
            $mod_width, $r->{module},
            $br_act, $r->{branch_req} * 100,
            $in_act, $r->{instr_req}  * 100;
    }
}

exit( @fails ? 1 : 0 );

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

sub _slurp {
    my ($path) = @_;
    open( my $fh, '<', $path ) or die "Cannot open $path: $!\n";
    local $/;
    return <$fh>;
}

sub _rel {
    my ( $path, $base ) = @_;
    $path =~ s{^\Q$base\E/?}{};
    return $path || '.';
}
