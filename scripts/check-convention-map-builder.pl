#!/usr/bin/env perl
# Detect Map.of() / ImmutableMap.of() calls that are multi-line AND have 2+
# entries.  Single-line calls are acceptable regardless of entry count.
#
# Rule (from CONVENTIONS.MD — "Map builder readability"):
#   A Map.of( / ImmutableMap.of( call that spans more than one line and
#   contains 2+ key/value pairs must be rewritten as
#   ImmutableMap.builder().put(...).put(...).build() for readability.
#
# The argument list is parsed with a full tokeniser (string literals, char
# literals, line comments, block comments) so commas inside strings or
# commented-out arguments never cause false positives.
#
# Usage:  perl scripts/check-convention-map-builder.pl [file|dir ...]
#         Directories are searched recursively for *.java.
# Exit:   0 = clean, 1 = violations found.

use strict;
use warnings;
use File::Find;

my @targets = @ARGV ? @ARGV : ('.');
my @files;
for my $t (@targets) {
    if   (-f $t) { push @files, $t }
    else { find(sub { push @files, $File::Find::name if /\.java$/ }, $t) }
}

my $total = 0;
for my $file (sort @files) {
    open my $fh, '<', $file or die "Cannot open $file: $!";
    local $/;
    my $content = <$fh>;
    close $fh;

    my $pos = 0;
    my $len = length $content;

    while ($pos < $len) {
        my $ch = substr($content, $pos, 1);

        # Skip masking constructs
        if ($ch eq '"') {
            $pos = substr($content, $pos, 3) eq '"""'
                 ? skip_text_block($content, $pos)
                 : skip_string($content, $pos);
            next;
        }
        if ($ch eq "'") { $pos = skip_char_lit($content, $pos); next }
        if ($pos + 1 < $len) {
            my $two = substr($content, $pos, 2);
            if ($two eq '//') { $pos = skip_line_comment($content,  $pos); next }
            if ($two eq '/*') { $pos = skip_block_comment($content, $pos); next }
        }

        # Detect (Immutable)?Map.of(
        if ($pos == 0 || substr($content, $pos - 1, 1) !~ /[A-Za-z0-9_]/) {
            if (substr($content, $pos) =~ /\A((?:Immutable)?Map)\s*\.\s*of\s*\(/) {
                my $call_name  = $1 . '.of';
                my $call_start = $pos;
                my $args_start = $pos + length($&);

                my ($args_end, @args) = extract_args($content, $args_start);

                # Violation: 2+ entries (4+ args) AND the call spans multiple lines.
                if (@args >= 4 && @args % 2 == 0) {
                    my $span = substr($content, $call_start, $args_end - $call_start);
                    if ($span =~ /\n/) {
                        my $lineno = 1 + (() = substr($content, 0, $call_start) =~ /\n/g);
                        my $entries = @args / 2;
                        print "$file:$lineno: '$call_name(' with $entries entries spans"
                            . " multiple lines -- prefer ImmutableMap.builder().put(\342\200\246).build()\n";
                        $total++;
                    }
                }

                $pos = $args_end;
                next;
            }
        }

        $pos++;
    }
}

print "\n$total map-builder convention violation(s) found.\n" if $total;
exit($total ? 1 : 0);

# ============================================================
# Tokeniser helpers (shared with fix-convention-map-builder.pl)
# ============================================================

sub skip_string {
    my ($s, $p) = @_;
    $p++;
    my $len = length $s;
    while ($p < $len) {
        my $c = substr($s, $p, 1);
        if ($c eq '\\') { $p += 2; next }
        if ($c eq '"')  { return $p + 1 }
        $p++;
    }
    return $p;
}

sub skip_text_block {
    my ($s, $p) = @_;
    $p += 3;
    my $len = length $s;
    while ($p <= $len - 3) {
        return $p + 3 if substr($s, $p, 3) eq '"""';
        $p++;
    }
    return $len;
}

sub skip_char_lit {
    my ($s, $p) = @_;
    $p++;
    my $len = length $s;
    while ($p < $len) {
        my $c = substr($s, $p, 1);
        if ($c eq '\\') { $p += 2; next }
        if ($c eq "'")  { return $p + 1 }
        $p++;
    }
    return $p;
}

sub skip_line_comment {
    my ($s, $p) = @_;
    my $nl = index($s, "\n", $p);
    return $nl < 0 ? length($s) : $nl + 1;
}

sub skip_block_comment {
    my ($s, $p) = @_;
    my $end = index($s, '*/', $p + 2);
    return $end < 0 ? length($s) : $end + 2;
}

sub extract_args {
    my ($s, $p) = @_;
    my $len   = length $s;
    my $depth = 1;
    my @args;
    my $arg_start = $p;

    while ($p < $len) {
        my $ch = substr($s, $p, 1);

        if ($ch eq '"') {
            $p = substr($s, $p, 3) eq '"""'
               ? skip_text_block($s, $p)
               : skip_string($s, $p);
            next;
        }
        if ($ch eq "'") { $p = skip_char_lit($s, $p); next }
        if ($p + 1 < $len) {
            my $two = substr($s, $p, 2);
            if ($two eq '//') { $p = skip_line_comment($s,  $p); next }
            if ($two eq '/*') { $p = skip_block_comment($s, $p); next }
        }

        if ($ch =~ /[(\[{]/) { $depth++; $p++; next }
        if ($ch =~ /[)\]}]/) {
            $depth--;
            if ($depth == 0) {
                push @args, substr($s, $arg_start, $p - $arg_start);
                return ($p + 1, @args);
            }
            $p++; next;
        }
        if ($ch eq ',' && $depth == 1) {
            push @args, substr($s, $arg_start, $p - $arg_start);
            $p++;
            $arg_start = $p;
            next;
        }
        $p++;
    }
    return ($p, @args);
}
