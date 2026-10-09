#!/usr/bin/env python
r"""Refuse to run the pipeline when the delivery is not a new, complete one.

An unattended job downloads a vendor file, stages it and republishes it. This
tool answers the question asked before any of that: is the delivery in front of
me new, and is it whole. It compares the size and modification time of every
expected member against a small state file. That file is written after the last
successful publish. One of five states comes back. FRESH and FIRST_RUN let the
pipeline start. UNCHANGED, REGRESSED and INCOMPLETE stop it.

The obvious tool is a newer-than check, remembered in one variable, and it is
wrong in two directions. A vendor export job that fails and restores last
week's file moves the timestamp BACKWARDS, and a not-equal comparison reads
that as a new delivery. A file that is still uploading has a timestamp that
already moved, and a size that will move again in four minutes. The neighbours
in this portfolio are closer and still do not cover it. jobharness gives an
unattended script logging, retry and resume, so it guards the STEPS of this
run. safe-republish refuses to truncate a target when the staged replacement is
implausible, so it guards the LOAD. It is blind by construction to a staged
table that is complete, plausible and a week old, because every count matches.
feedstamp guards the DELIVERY, before the load and before arcpy is imported. It
does not check sidecar files: a shapefile missing its .prj is fcload's refusal
and gdbfence's, and reimplementing that here would only disagree with them.

    python feedstamp.py --self-test
    python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect data.zip
    python feedstamp.py --dir staging --expect data.zip --state staging/.feedstamp.json
    python feedstamp.py --dir staging --expect data.zip --state s.json --record
    python feedstamp.py --stamp data.zip:4194304:20260715120000 --expect data.zip --state s.json
    python feedstamp.py --dir staging --expect data.zip --min-quiet 300 --json

Exit codes: 0 the delivery may be consumed, 1 refused, 2 the state file could
not be written, 64 usage error.

WHY THE STATE FILE IS WRITTEN SEPARATELY. --record is this tool's --apply, and
it belongs after the publish, not before it. A check run writes nothing. If the
check run stamped the delivery, a pipeline that died between staging and
publish would have marked that delivery consumed. The rerun of the same file
the next night would then report UNCHANGED and refuse for ever. --record also
refuses to write after a refusal, so a state file never records a delivery the
pipeline was told not to use.

WHY MDTM IS NOT A LOCAL TIME. FTP's MDTM reply is UTC by protocol, and it
carries no zone (RFC 3659 section 3). The two natural ways to turn it into a
number both read it as local time:

    time.mktime(time.strptime(text, "%Y%m%d%H%M%S"))                 # wrong
    datetime.datetime.strptime(text, "%Y%m%d%H%M%S").timestamp()     # wrong

On a host that keeps summer time, those two spellings of the same instant sit
an hour apart across the year. An hour is enough to make a delivery that landed
at 00:30 look older than the one before it. This tool then refuses it as
REGRESSED, every night from March to November. calendar.timegm is the reading
that has no zone in it, and the self-test pins both halves of the year.
"""

from __future__ import print_function

import argparse
import calendar
import collections
import io
import json
import os
import shutil
import sys
import tempfile
import time

# =============================================================================
# CONFIGURATION. Deliberately not flags. Change here, not at the call site.
# =============================================================================

# Two modification times this far apart or closer are the same time. FAT and
# exFAT store a modification time to the nearest two seconds, so a file that
# came across one of them can come back rounded. One second is too tight for
# that and reports a delivery that never moved as a new one.
MTIME_EPSILON = 2.0

# The floor a member must clear when --expect gives no size of its own. A
# delivery of zero bytes is incomplete whatever its timestamp says.
DEFAULT_MIN_BYTES = 1

# The state file format. read_state refuses anything else rather than guessing
# at fields that may have meant something different.
STATE_VERSION = 1

# The highest epoch second this tool accepts as a modification time: midnight
# on 1 January 3000. Windows' time.gmtime raises OSError above roughly this
# point while Linux keeps going to the year 9999, so a stamp past it would
# make the report crash on one host and print on the other. NTFS will hand
# back such a value: os.utime with 1e17 comes back as the year 8318.
MAX_EPOCH = 32503680000.0

# =============================================================================
# End of CONFIGURATION.
# =============================================================================

# The five states. Two of them let the pipeline start.
FRESH = "FRESH"
FIRST_RUN = "FIRST_RUN"
UNCHANGED = "UNCHANGED"
REGRESSED = "REGRESSED"
INCOMPLETE = "INCOMPLETE"

# The wire format of an FTP MDTM reply, and its length in digits.
MDTM_FORMAT = "%Y%m%d%H%M%S"
MDTM_DIGITS = 14

Observation = collections.namedtuple("Observation", "name size mtime")
Expect = collections.namedtuple("Expect", "name min_bytes")
Result = collections.namedtuple("Result", "state reasons")


# ----------------------------------------------------------------- pure core

def parse_mdtm(text):
    """Turn an FTP MDTM timestamp into epoch seconds, reading it as UTC.

    Accepts the bare 14 digits, the whole "213 <stamp>" reply line, and the
    fractional seconds some servers add. calendar.timegm is the point of the
    function: it has no local zone in it. See the module docstring.
    """
    text = text.strip()
    if text.upper().startswith("213 "):
        text = text[4:].strip()
    digits = text.split(".")[0]
    if len(digits) != MDTM_DIGITS or not digits.isdigit():
        raise ValueError("not an MDTM timestamp: %r" % text)
    return float(calendar.timegm(time.strptime(digits, MDTM_FORMAT)))


def parse_mtime(text):
    """Read a modification time given on the command line.

    Exactly 14 digits is an MDTM stamp in UTC. Anything else is epoch seconds,
    which is what os.stat gives. A 14 digit epoch is the year 446000, so the
    two spellings cannot be confused.
    """
    text = text.strip()
    # A caller who pipes sendcmd("MDTM name") straight through hands over the
    # whole reply line, space and all, so the prefix is taken off before the
    # shape of what is left decides how to read it.
    head = text[4:].strip() if text.upper().startswith("213 ") else text
    head = head.split(".")[0]
    if len(head) == MDTM_DIGITS and head.isdigit():
        value = parse_mdtm(text)
    else:
        try:
            value = float(text)
        except ValueError:
            raise ValueError("not epoch seconds or an MDTM stamp: %r" % text)
    # float() accepts "nan" and "inf", and a probe that misread a reply can
    # hand either one over. Refusing at the boundary is the whole point: a
    # value that cannot be compared must never reach the decision.
    if not usable_mtime(value):
        raise ValueError("not a usable modification time: %r" % text)
    return value


def usable_mtime(value):
    """Is this a modification time the tool can compare and then print?

    NaN is the one that matters. Every comparison against NaN is False, so a
    NaN reaching freshness() misses the regression test, misses the unchanged
    test, and falls through to FRESH: the guard says GO on a number that means
    nothing. Infinity and an epoch past MAX_EPOCH are the other half. They
    compare fine and then take the run down inside format_stamp, which turns a
    verdict into a traceback in a scheduled job.

    A range test answers all of them at once, because every comparison against
    NaN or -inf is already False. Anything before 1970 is a broken clock
    rather than a delivery.
    """
    return 0.0 <= value <= MAX_EPOCH


def format_stamp(epoch):
    """A timestamp for humans, always in UTC, so two runs print comparably.

    Every caller has passed usable_mtime first, which is what keeps this total.
    """
    return time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(epoch))


def member_key(name):
    """The key two spellings of the same member must share.

    Only the last path component counts, because the vendor sends a name and
    the staging directory holds a path.

    ponytail: the key is case folded, so ROADS.ZIP and roads.zip are one
    member. That is right on Windows and on the servers this was written for,
    and wrong on a case sensitive server that really does deliver both.
    Upgrade path: record the source's case rule in the state file and fold
    only when it says to.
    """
    return name.replace("\\", "/").rsplit("/", 1)[-1].lower()


def parse_expect(text):
    """Read one --expect value: NAME, or NAME:MINBYTES."""
    name, sep, tail = text.rpartition(":")
    if sep and name and tail.isdigit():
        return Expect(name, int(tail))
    if not text.strip():
        raise ValueError("--expect wants a member name")
    return Expect(text, DEFAULT_MIN_BYTES)


def parse_stamp(text):
    """Read one --stamp value: NAME:SIZE:MTIME.

    This is the seam for a probe this tool does not make itself. An ftplib
    caller that already holds SIZE and MDTM for the remote file feeds them in
    here and gets the decision without downloading anything.
    """
    parts = text.rsplit(":", 2)
    if len(parts) != 3:
        raise ValueError("--stamp wants NAME:SIZE:MTIME, got %r" % text)
    name, size, mtime = parts
    if not name.strip():
        raise ValueError("--stamp wants a member name, got %r" % text)
    try:
        size_n = int(size)
    except ValueError:
        raise ValueError("size is not a whole number of bytes: %r" % text)
    if size_n < 0:
        raise ValueError("size cannot be negative: %r" % text)
    return Observation(name.strip(), size_n, parse_mtime(mtime))


def index_observations(observed):
    """Key what is on the server, or on disk, by member name."""
    return dict((member_key(o.name), o) for o in observed)


def completeness(expects, observed, now=None, min_quiet=0):
    """Reasons this delivery is not whole yet. An empty list means it is.

    The order inside one member matters: absent beats empty beats short beats
    an unreadable stamp beats still settling, so the report names the first
    thing wrong with it rather than every consequence of that thing.
    """
    seen = index_observations(observed)
    reasons = []
    for e in expects:
        obs = seen.get(member_key(e.name))
        if obs is None:
            reasons.append("%s is missing from the delivery" % e.name)
        elif obs.size <= 0:
            reasons.append("%s is zero bytes" % e.name)
        elif obs.size < e.min_bytes:
            reasons.append("%s is %d bytes, under the %d byte floor for it"
                           % (e.name, obs.size, e.min_bytes))
        elif not usable_mtime(obs.mtime):
            # The os.stat path, which no command line and no state file saw.
            # A filesystem really does return these: NTFS turns an out of
            # range utime into the year 8318. A delivery whose age cannot be
            # judged is one the pipeline must not consume.
            reasons.append("%s carries a modification time that cannot be "
                           "read as a date: %r" % (e.name, obs.mtime))
        elif now is not None and min_quiet > 0 and (now - obs.mtime) < min_quiet:
            if obs.mtime > now:
                reasons.append(
                    "%s is stamped %s, ahead of this machine's clock, so its "
                    "age cannot be judged" % (e.name, format_stamp(obs.mtime)))
            else:
                reasons.append(
                    "%s was written %d second(s) ago, inside the %d second "
                    "quiet period" % (e.name, int(now - obs.mtime), min_quiet))
    return reasons


def freshness(expects, observed, prior, epsilon=MTIME_EPSILON):
    """Compare a complete delivery against the one that was last published.

    Returns REGRESSED, UNCHANGED or FRESH. Call completeness first: this half
    assumes every expected member is present, because a member that is not
    there has nothing to compare and a verdict of its own already.
    """
    seen = index_observations(observed)
    regressed = []
    moved = []
    matched = 0
    for e in expects:
        obs = seen[member_key(e.name)]
        was = prior.get(member_key(e.name))
        if was is None:
            moved.append("%s has no stamp in the state file yet" % e.name)
        elif obs.mtime < was["mtime"] - epsilon:
            # The case a newer-than check reads as a new delivery. A vendor
            # job that failed and restored an older file lands here.
            regressed.append(
                "%s is stamped %s, older than the %s recorded after the last "
                "publish" % (e.name, format_stamp(obs.mtime),
                             format_stamp(was["mtime"])))
        elif obs.size != was["size"]:
            # A size that moved is a new delivery even when the timestamp did
            # not. A vendor that rebuilds a file in place can hold the mtime.
            moved.append("%s is %d bytes, was %d bytes"
                         % (e.name, obs.size, was["size"]))
        elif abs(obs.mtime - was["mtime"]) <= epsilon:
            matched += 1
        else:
            moved.append("%s is stamped %s, was %s"
                         % (e.name, format_stamp(obs.mtime),
                            format_stamp(was["mtime"])))
    if regressed:
        return Result(REGRESSED, regressed)
    if matched == len(expects):
        return Result(UNCHANGED, ["every expected member has the size and "
                                  "timestamp recorded after the last publish"])
    return Result(FRESH, moved)


def classify(expects, observed, prior, now=None, min_quiet=0,
             prior_note=None, epsilon=MTIME_EPSILON):
    """The whole decision: FRESH, FIRST_RUN, UNCHANGED, REGRESSED or INCOMPLETE.

    Completeness is asked first and answers on its own. A delivery that is half
    uploaded is incomplete whatever its timestamp says, and comparing half a
    file against last week's stamp only adds a second, wrong answer.
    """
    if not expects:
        raise ValueError("classify needs at least one expected member")
    reasons = completeness(expects, observed, now=now, min_quiet=min_quiet)
    if reasons:
        return Result(INCOMPLETE, reasons)
    if not prior:
        return Result(FIRST_RUN, [prior_note or "no prior state file, so there"
                                  " is nothing to compare this delivery against"])
    return freshness(expects, observed, prior, epsilon=epsilon)


def verdict(result, allow_unchanged=False):
    """Turn a state into (exit code, may the pipeline run).

    UNCHANGED is the only state whose answer is an operator policy. A feed that
    is rebuilt every night is broken when it does not move; a feed that changes
    only when the county changes it is not.
    """
    if result.state in (FRESH, FIRST_RUN):
        return 0, True
    if result.state == UNCHANGED and allow_unchanged:
        return 0, True
    return 1, False


def describe(result, allow_unchanged=False):
    """The report as lines, and the exit code, without printing anything."""
    code, allowed = verdict(result, allow_unchanged)
    lines = ["feedstamp: %s" % result.state]
    for reason in result.reasons:
        lines.append("    %s" % reason)
    if result.state == UNCHANGED and allowed:
        lines.append("    --allow-unchanged is set, so this is not a refusal")
    lines.append("VERDICT: %s" % ("GO" if allowed else "REFUSE"))
    return lines, code


def build_state(observed, recorded_at=None):
    """The document written after a publish succeeds.

    It holds delivered file names, sizes and timestamps, which is a description
    of a vendor feed. Keep it beside the staging directory and out of git.
    """
    members = {}
    for o in observed:
        members[o.name] = {"size": o.size, "mtime": o.mtime}
    return {"feedstamp": STATE_VERSION,
            "recorded": float(time.time() if recorded_at is None
                              else recorded_at),
            "members": members}


def read_state(text):
    """Return (members, note). members is None when the file cannot be trusted.

    Every failure here fails to the same place: no memory, so the run is a
    first run and the reason is printed. Raising instead would stop a pipeline
    over the guard's own bookkeeping file, which is a worse outcome than the
    one this tool exists to prevent.
    """
    try:
        doc = json.loads(text)
    except ValueError:
        return None, ("the state file is not readable JSON, so this run is "
                      "treated as a first run")
    if not isinstance(doc, dict) or doc.get("feedstamp") != STATE_VERSION:
        return None, ("the state file is not a feedstamp version %d state "
                      "file, so this run is treated as a first run"
                      % STATE_VERSION)
    members = doc.get("members")
    if not isinstance(members, dict) or not members:
        return None, ("the state file records no members, so this run is "
                      "treated as a first run")
    out = {}
    for name, rec in members.items():
        try:
            mtime = float(rec["mtime"])
            # json.loads accepts the bare token NaN, and json.dump writes it,
            # so a state file can carry one round trip to round trip. It is
            # damage like any other damage here.
            if not usable_mtime(mtime):
                raise ValueError("unusable mtime")
            out[member_key(name)] = {"name": name,
                                     "size": int(rec["size"]),
                                     "mtime": mtime}
        except (TypeError, ValueError, KeyError):
            return None, ("the state file entry for %s is damaged, so this "
                          "run is treated as a first run" % name)
    return out, None


def json_report(result, allow_unchanged=False, observed=()):
    """The same decision as one JSON object, for a caller that parses it."""
    code, allowed = verdict(result, allow_unchanged)
    return json.dumps({
        "state": result.state,
        "verdict": "GO" if allowed else "REFUSE",
        "exit_code": code,
        "reasons": list(result.reasons),
        "observed": [{"name": o.name, "size": o.size,
                      "mtime": format_stamp(o.mtime)} for o in observed],
    }, indent=2, sort_keys=True)


# ------------------------------------------------------------------------ io

def observe_directory(path, expects):
    """Stat every expected member in a staging directory.

    A member that is not there is simply absent from the result. Deciding what
    that means belongs to the core, which has one place for it.
    """
    observed = []
    for e in expects:
        full = os.path.join(path, e.name)
        try:
            st = os.stat(full)
        except OSError:
            continue
        observed.append(Observation(e.name, st.st_size, st.st_mtime))
    return observed


def load_state(path):
    """Read the state file if there is one. Returns (members, note)."""
    if not path or not os.path.isfile(path):
        return None, None
    try:
        handle = open(path, "r")
    except OSError as exc:
        return None, ("the state file could not be opened (%s), so this run "
                      "is treated as a first run" % exc)
    try:
        text = handle.read()
    finally:
        handle.close()
    return read_state(text)


def save_state(path, doc):
    """Write the state file, through a temporary name.

    A publish that succeeded and a state file that is half written is the one
    way this tool could cause the failure it guards against, so the rename is
    the only thing the next run can see.
    """
    tmp = path + ".tmp"
    handle = open(tmp, "w")
    try:
        json.dump(doc, handle, indent=2, sort_keys=True)
        handle.write("\n")
    finally:
        handle.close()
    os.replace(tmp, path)


# ------------------------------------------------------------------ self-test

# Fixtures. Every name, size and timestamp below is invented for this file.
# JUL and JAN are the same wall clock time in the two halves of a year, which
# is where a local-time reading of an MDTM stamp loses an hour.
JUL_MDTM = "20260715120000"
JAN_MDTM = "20260115120000"
JUL_EPOCH = 1784116800.0
JAN_EPOCH = 1768478400.0
HALF_YEAR = 15638400.0          # 181 days, in seconds, with no DST step in it
DAY = 86400.0


def summarise(passed, failed):
    """The closing lines of a self-test run, and its exit code.

    Pulled out of the run so both shapes can be asserted. A footer that is
    only ever reached by a green run is a footer nobody has read.
    """
    total = passed + len(failed)
    if failed:
        lines = ["%d assertions, %d failed" % (total, len(failed))]
        for label in failed:
            lines.append("  FAILED: %s" % label)
        return lines, 1
    return ["%d assertions, 0 failed" % total], 0


def self_test():
    """Assertions over the decision core, then over the io layer and the cli.

    Nothing here reaches the network, a database or arcpy. The io half writes
    into a temporary directory of its own and removes it again.
    """
    passed = [0]
    failed = []

    def check(cond, label):
        if cond:
            passed[0] += 1
            print("PASS  %s" % label)
        else:
            failed.append(label)
            print("FAIL  %s" % label)

    def raises(fn, label):
        try:
            fn()
        except ValueError:
            check(True, label)
        except Exception as exc:
            check(False, "%s (wrong exception %r)" % (label, exc))
        else:
            check(False, "%s (no error raised)" % label)

    def expects(*items):
        return [parse_expect(t) for t in items]

    def obs(name, size, mtime):
        return Observation(name, size, mtime)

    def prior_of(*observations):
        members, note = read_state(json.dumps(build_state(list(observations))))
        return members

    print("feedstamp self-test: no network, no arcpy, no database")
    print("-" * 68)

    # ---- THE PINNED PAIR: an MDTM stamp is UTC, in both halves of the year
    check(parse_mdtm(JUL_MDTM) == JUL_EPOCH,
          "a july MDTM stamp reads as UTC, not as local time  <-- pinned defect")
    check(parse_mdtm(JAN_MDTM) == JAN_EPOCH,
          "a january MDTM stamp reads as UTC too  <-- pinned defect")
    check(parse_mdtm(JUL_MDTM) - parse_mdtm(JAN_MDTM) == HALF_YEAR,
          "the two halves of the year are 181 days apart, with no summer "
          "time step in between  <-- pinned defect")

    def naive_local(stamp):
        # The reading this tool must not use, written out so the assertion
        # below can measure what it costs on the host running the test.
        return time.mktime(time.strptime(stamp, MDTM_FORMAT))

    def local_offset(epoch):
        lt = time.localtime(epoch)
        return -(time.altzone if lt.tm_isdst > 0 else time.timezone)

    dst_shift = local_offset(JUL_EPOCH) - local_offset(JAN_EPOCH)
    check(naive_local(JUL_MDTM) - naive_local(JAN_MDTM) == HALF_YEAR - dst_shift,
          "the local-time reading of those same two stamps loses this host's "
          "summer time shift of %d second(s)  <-- pinned defect" % dst_shift)
    check(format_stamp(JUL_EPOCH) == "2026-07-15 12:00:00Z",
          "a stamp is reported in UTC, so two hosts print one delivery alike")

    # ---- reading what a server or a command line hands over
    check(parse_mdtm("213 " + JUL_MDTM) == JUL_EPOCH,
          "a whole 213 reply line is accepted, not only the digits")
    check(parse_mdtm(JUL_MDTM + ".137") == JUL_EPOCH,
          "the fractional seconds some servers add are dropped")
    check(parse_mdtm("  " + JUL_MDTM + "  ") == JUL_EPOCH,
          "surrounding whitespace is trimmed")
    raises(lambda: parse_mdtm("2026071512"), "a stamp of ten digits is refused")
    raises(lambda: parse_mdtm("2026071512000x"),
           "a stamp with a letter in it is refused")
    raises(lambda: parse_mdtm("20261532120000"),
           "month 15 is refused rather than wrapped into the next year")
    check(parse_mtime("1784116800") == JUL_EPOCH,
          "a plain number is epoch seconds, which is what os.stat gives")
    check(parse_mtime(JUL_MDTM) == JUL_EPOCH,
          "exactly 14 digits is an MDTM stamp, never an epoch")
    check(parse_mtime("1784116800.5") == JUL_EPOCH + 0.5,
          "a fractional epoch keeps its fraction")
    check(parse_mtime("213 " + JUL_MDTM) == JUL_EPOCH,
          "a whole MDTM reply line passed straight through is read as one too")
    raises(lambda: parse_mtime("yesterday"),
           "a word where a time belongs is a usage error, not a zero")

    # ---- THE PINNED DEFECT: a timestamp that is not a number at all.
    # float() takes "nan", every comparison against NaN is False, and a NaN
    # that reached freshness() missed both tests there and came out FRESH.
    raises(lambda: parse_mtime("nan"),
           "a NaN timestamp is refused at the boundary, because every "
           "comparison against it is False and the verdict would be GO"
           "  <-- pinned defect")
    raises(lambda: parse_mtime("inf"),
           "so is an infinite one  <-- pinned defect")
    raises(lambda: parse_mtime("-1"),
           "and one from before 1970, which is a broken clock")
    raises(lambda: parse_stamp("polygons.zip:10:nan"),
           "a --stamp carrying NaN is refused with it  <-- pinned defect")
    check(usable_mtime(MAX_EPOCH) and not usable_mtime(MAX_EPOCH + 1.0),
          "the ceiling is the last epoch second both Windows and Linux can "
          "print  <-- pinned defect")
    check(usable_mtime(0.0) and not usable_mtime(-0.5),
          "and the floor is the epoch itself")
    check(format_stamp(MAX_EPOCH) == "3000-01-01 00:00:00Z",
          "which format_stamp renders rather than raising OSError on")

    # ---- the manifest
    check(parse_expect("polygons.zip") == Expect("polygons.zip", 1),
          "an expected member with no floor gets the one byte default")
    check(parse_expect("polygons.zip:1000000") == Expect("polygons.zip", 1000000),
          "a floor after the last colon is read as bytes")
    check(parse_expect("polygons.zip:0") == Expect("polygons.zip", 0),
          "a floor of zero is allowed, and the zero byte rule still applies")
    check(parse_expect("delivery:2026:500") == Expect("delivery:2026", 500),
          "only the last colon separates the floor, so a name may contain one")
    check(parse_expect("archive/polygons.zip").name == "archive/polygons.zip",
          "a member named with a subdirectory keeps its path")
    raises(lambda: parse_expect("   "), "an empty member name is refused")

    # ---- an observation handed in from a probe this tool does not make
    check(parse_stamp("polygons.zip:4194304:" + JUL_MDTM)
          == obs("polygons.zip", 4194304, JUL_EPOCH),
          "a stamp of name, size and MDTM becomes one observation")
    check(parse_stamp("polygons.zip:4194304:1784116800").mtime == JUL_EPOCH,
          "the same stamp given in epoch seconds lands on the same instant")
    check(parse_stamp("polygons.zip:4194304:213 " + JUL_MDTM).mtime == JUL_EPOCH,
          "and so does the raw reply line an ftplib caller already holds")
    raises(lambda: parse_stamp("polygons.zip:4194304"),
           "a stamp missing its timestamp is refused")
    raises(lambda: parse_stamp("polygons.zip:large:1784116800"),
           "a size that is not a number is refused")
    raises(lambda: parse_stamp("polygons.zip:-1:1784116800"),
           "a negative size is refused")
    raises(lambda: parse_stamp(":10:1784116800"),
           "a stamp with no member name is refused")
    check(member_key("staging\\Polygons.ZIP") == "polygons.zip",
          "a windows path and a bare lowercase name are the same member")
    check(member_key("staging/polygons.zip") == member_key("POLYGONS.ZIP"),
          "so are a forward slash path and an uppercase name")

    # ---- incompleteness answers on its own, whatever the timestamp says
    man = expects("polygons.zip:1000000", "attributes.zip:1000000")
    both = [obs("polygons.zip", 4194304, JUL_EPOCH),
            obs("attributes.zip", 2097152, JUL_EPOCH)]
    check(completeness(man, both) == [],
          "a delivery with both members over their floors is complete")
    reasons = completeness(man, [obs("polygons.zip", 4194304, JUL_EPOCH)])
    check(len(reasons) == 1 and "attributes.zip is missing" in reasons[0],
          "a missing manifest member is named, not just counted  <-- pinned defect")
    empty = [obs("polygons.zip", 0, JUL_EPOCH),
             obs("attributes.zip", 2097152, JUL_EPOCH)]
    check(classify(man, empty, prior_of(*both)).state == INCOMPLETE,
          "a zero byte member is INCOMPLETE against a full prior state"
          "  <-- pinned defect")
    check(completeness(expects("polygons.zip:0"),
                       [obs("polygons.zip", 0, JUL_EPOCH)])
          == ["polygons.zip is zero bytes"],
          "a zero byte member is INCOMPLETE even when its floor is zero"
          "  <-- pinned defect")
    short = [obs("polygons.zip", 4194304, JUL_EPOCH),
             obs("attributes.zip", 9, JUL_EPOCH)]
    check(completeness(man, short)
          == ["attributes.zip is 9 bytes, under the 1000000 byte floor for it"],
          "a member under its floor is reported with both numbers")
    check(completeness(man, [obs("polygons.zip", 1000000, JUL_EPOCH),
                             obs("attributes.zip", 1000000, JUL_EPOCH)]) == [],
          "a member exactly on its floor passes")
    check(len(completeness(man, [obs("polygons.zip", 999999, JUL_EPOCH),
                                 obs("attributes.zip", 1000000, JUL_EPOCH)])) == 1,
          "and one byte under that floor is INCOMPLETE, so the floor is the "
          "lowest size that passes")
    check(len(completeness(man, [])) == 2,
          "an empty delivery names every member it is missing")
    check(completeness(expects("polygons.zip"), [])
          == ["polygons.zip is missing from the delivery"],
          "a member that is absent is not also reported as zero bytes")

    # ---- the file that is still uploading
    now = JUL_EPOCH + 30.0
    late = [obs("polygons.zip", 4194304, JUL_EPOCH)]
    check(completeness(expects("polygons.zip"), late, now=now, min_quiet=300)
          == ["polygons.zip was written 30 second(s) ago, inside the 300 "
              "second quiet period"],
          "a member written 30 seconds ago is still settling, so INCOMPLETE")
    check(completeness(expects("polygons.zip"), late, now=JUL_EPOCH + 301.0,
                       min_quiet=300) == [],
          "the same member five minutes later is complete")
    check(completeness(expects("polygons.zip"), late, now=JUL_EPOCH + 300.0,
                       min_quiet=300) == [],
          "a member exactly as old as the quiet period has served it out")
    check(len(completeness(expects("polygons.zip"), late,
                           now=JUL_EPOCH + 299.5, min_quiet=300)) == 1,
          "and half a second short of it is still settling")
    check(completeness(expects("polygons.zip"), late, now=now, min_quiet=0) == [],
          "the quiet period is off by default, so the check is deterministic")
    check(completeness(expects("polygons.zip"), late, now=None, min_quiet=300)
          == [],
          "and off when the caller supplies no clock")
    reasons = completeness(expects("polygons.zip"),
                           [obs("polygons.zip", 4194304, JUL_EPOCH + 3600.0)],
                           now=JUL_EPOCH, min_quiet=300)
    check(len(reasons) == 1 and "ahead of this machine" in reasons[0],
          "a member stamped in the future is reported as a clock problem, "
          "not as a negative age")
    # os.stat is the one source of mtimes that no boundary above validated.
    # NTFS returns the year 8318 for an out of range utime, and time.gmtime
    # raises OSError on it, which used to end the run in a traceback.
    unreadable = completeness(expects("polygons.zip"),
                              [obs("polygons.zip", 4194304, MAX_EPOCH + 1.0)])
    check(len(unreadable) == 1 and "cannot be read as a date" in unreadable[0],
          "a member whose on-disk stamp is past the year 3000 is INCOMPLETE, "
          "not a traceback inside the report  <-- pinned defect")
    check(classify(expects("polygons.zip"),
                   [obs("polygons.zip", 4194304, float("nan"))],
                   prior_of(obs("polygons.zip", 4194304, JUL_EPOCH))).state
          == INCOMPLETE,
          "and a NaN one is INCOMPLETE rather than FRESH  <-- pinned defect")
    check(completeness(expects("polygons.zip"),
                       [obs("polygons.zip", 0, float("nan"))])
          == ["polygons.zip is zero bytes"],
          "an empty member is still reported as empty first, because the "
          "stamp is not the first thing wrong with it")

    # ---- THE PINNED DEFECT: a timestamp that moved backwards
    last_week = prior_of(obs("polygons.zip", 4194304, JUL_EPOCH),
                         obs("attributes.zip", 2097152, JUL_EPOCH))
    restored = [obs("polygons.zip", 4194304, JUL_EPOCH - 7 * DAY),
                obs("attributes.zip", 2097152, JUL_EPOCH - 7 * DAY)]
    back = classify(man, restored, last_week)
    check(back.state == REGRESSED,
          "a delivery stamped a week earlier than the last published one is "
          "REGRESSED  <-- pinned defect")
    check(back.reasons[0].startswith("polygons.zip is stamped 2026-07-08")
          and "older than the 2026-07-15" in back.reasons[0],
          "and the refusal names both stamps, so the operator can see which "
          "week arrived")
    check(len(back.reasons) == 2,
          "both members that moved backwards are named")
    check(restored[0].mtime != last_week["polygons.zip"]["mtime"],
          "a naive changed-since check reads that same pair as a delivery "
          "worth loading  <-- pinned defect")
    check(verdict(back, allow_unchanged=True) == (1, False),
          "REGRESSED refuses even with --allow-unchanged set  <-- pinned defect")
    inside = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH - 1.0),
                            obs("attributes.zip", 2097152, JUL_EPOCH - 1.0)],
                      last_week)
    check(inside.state == UNCHANGED,
          "a one second step back is filesystem rounding, not a regression")
    outside = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH - 3.0),
                             obs("attributes.zip", 2097152, JUL_EPOCH)],
                       last_week)
    check(outside.state == REGRESSED,
          "three seconds back is past the rounding tolerance and refuses")
    mixed = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH + DAY),
                           obs("attributes.zip", 2097152, JUL_EPOCH - DAY)],
                     last_week)
    check(mixed.state == REGRESSED,
          "one member moving forward does not excuse another moving back")
    half = [obs("polygons.zip", 0, JUL_EPOCH - 7 * DAY),
            obs("attributes.zip", 2097152, JUL_EPOCH - 7 * DAY)]
    check(classify(man, half, last_week).state == INCOMPLETE,
          "incompleteness is decided first, so a broken old delivery is "
          "reported as broken, not as old")

    # ---- unchanged, and the size that moves while the timestamp does not
    same = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH),
                          obs("attributes.zip", 2097152, JUL_EPOCH)], last_week)
    check(same.state == UNCHANGED,
          "yesterday's file still sitting there is UNCHANGED")
    check(verdict(same) == (1, False),
          "UNCHANGED refuses by default, because a nightly feed that did not "
          "move is a feed that did not arrive")
    check(verdict(same, allow_unchanged=True) == (0, True),
          "--allow-unchanged turns that into a pass, because it is an "
          "operator policy and not a fact")
    jitter = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH + 1.5),
                            obs("attributes.zip", 2097152, JUL_EPOCH)],
                      last_week)
    check(jitter.state == UNCHANGED,
          "a stamp that moved 1.5 seconds with the size unchanged is still "
          "the same delivery")
    edge = classify(man, [obs("polygons.zip", 4194304,
                              JUL_EPOCH + MTIME_EPSILON),
                          obs("attributes.zip", 2097152, JUL_EPOCH)], last_week)
    check(edge.state == UNCHANGED,
          "a stamp exactly two seconds on is FAT rounding, so still UNCHANGED")
    check(classify(man, [obs("polygons.zip", 4194304,
                             JUL_EPOCH + MTIME_EPSILON + 0.01),
                         obs("attributes.zip", 2097152, JUL_EPOCH)],
                   last_week).state == FRESH,
          "and one hundredth of a second past the tolerance is a new delivery")
    check(classify(man, [obs("polygons.zip", 4194304,
                             JUL_EPOCH - MTIME_EPSILON),
                         obs("attributes.zip", 2097152, JUL_EPOCH)],
                   last_week).state == UNCHANGED,
          "the tolerance is symmetric, so exactly two seconds back is not a "
          "regression either")
    rebuilt = classify(man, [obs("polygons.zip", 4194999, JUL_EPOCH),
                             obs("attributes.zip", 2097152, JUL_EPOCH)],
                       last_week)
    check(rebuilt.state == FRESH,
          "a size that changed with the timestamp unchanged is FRESH"
          "  <-- pinned defect")
    check(rebuilt.reasons == ["polygons.zip is 4194999 bytes, was 4194304 bytes"],
          "and the report names the member whose size moved")
    moved_on = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH + DAY),
                              obs("attributes.zip", 2097152, JUL_EPOCH + DAY)],
                        last_week)
    check(moved_on.state == FRESH,
          "a delivery stamped a day later is FRESH")
    check(verdict(moved_on) == (0, True), "FRESH lets the pipeline start")
    check(len(moved_on.reasons) == 2 and "was 2026-07-15" in moved_on.reasons[1],
          "and each member reports the stamp it replaced")
    one_moved = classify(man, [obs("polygons.zip", 4194304, JUL_EPOCH + DAY),
                               obs("attributes.zip", 2097152, JUL_EPOCH)],
                         last_week)
    check(one_moved.state == FRESH,
          "one member of two moving forward is enough to be a new delivery")
    added = classify(expects("polygons.zip", "attributes.zip", "millage.zip"),
                     [obs("polygons.zip", 4194304, JUL_EPOCH),
                      obs("attributes.zip", 2097152, JUL_EPOCH),
                      obs("millage.zip", 40960, JUL_EPOCH)], last_week)
    check(added.state == FRESH and added.reasons
          == ["millage.zip has no stamp in the state file yet"],
          "a member added to the manifest is new, and is named as new")

    # ---- first run, and a state file that cannot be trusted
    first = classify(man, both, None)
    check(first.state == FIRST_RUN and verdict(first) == (0, True),
          "with no state file at all, the first run is allowed")
    broken, note = read_state("{ this is not json")
    check(broken is None and "not readable JSON" in note,
          "a corrupt state file returns a reason instead of raising"
          "  <-- pinned defect")
    fell_back = classify(man, both, broken, prior_note=note)
    check(fell_back.state == FIRST_RUN and fell_back.reasons == [note],
          "and the run falls back to FIRST_RUN carrying that reason"
          "  <-- pinned defect")
    check(read_state(json.dumps({"feedstamp": 99, "members": {}}))[0] is None,
          "a state file from another version is not read")
    check("version 1" in read_state(json.dumps({"feedstamp": 99}))[1],
          "and the reason says which version this tool writes")
    check(read_state(json.dumps({"feedstamp": 1}))[0] is None,
          "a state file with no members block is not read")
    check(read_state(json.dumps({"feedstamp": 1, "members": {}}))[0] is None,
          "nor is one that records no members")
    check(read_state(json.dumps([1, 2, 3]))[0] is None,
          "nor is a JSON document that is not an object")
    damaged = json.dumps({"feedstamp": 1, "members":
                          {"polygons.zip": {"size": "big", "mtime": 1.0}}})
    members, note = read_state(damaged)
    check(members is None and "polygons.zip is damaged" in note,
          "a damaged entry names the member it could not read")
    missing_field = json.dumps({"feedstamp": 1, "members":
                                {"polygons.zip": {"size": 10}}})
    check(read_state(missing_field)[0] is None,
          "an entry missing its timestamp is damaged too")
    not_a_record = json.dumps({"feedstamp": 1, "members": {"polygons.zip": 7}})
    check(read_state(not_a_record)[0] is None,
          "and so is an entry that is a number where a record belongs")
    # json.dump writes the bare token NaN and json.loads reads it back, so a
    # state file really can carry one from run to run. Every later comparison
    # against it is False, which read as FRESH before this was pinned.
    nan_state = json.dumps({"feedstamp": 1, "members":
                            {"polygons.zip": {"size": 10, "mtime": float("nan")}}})
    check("NaN" in nan_state,
          "json writes a NaN stamp as a bare token, so a state file can hold "
          "one  <-- pinned defect")
    members, note = read_state(nan_state)
    check(members is None and "polygons.zip is damaged" in note,
          "and reading it back is damage, not a stamp  <-- pinned defect")
    far_state = json.dumps({"feedstamp": 1, "members":
                            {"polygons.zip": {"size": 10,
                                              "mtime": MAX_EPOCH + 1.0}}})
    check(read_state(far_state)[0] is None,
          "a stamp past the year 3000 is damage too, because one host could "
          "not print it")
    raises(lambda: classify([], both, None),
           "classifying against an empty manifest is a programming error")

    # ---- the state document itself
    doc = build_state(both, recorded_at=JUL_EPOCH)
    check(doc["feedstamp"] == STATE_VERSION and doc["recorded"] == JUL_EPOCH,
          "the state document carries its version and when it was written")
    check(sorted(doc["members"]) == ["attributes.zip", "polygons.zip"],
          "and one entry per observed member")
    check(doc["members"]["polygons.zip"] == {"size": 4194304, "mtime": JUL_EPOCH},
          "each entry is the size and timestamp that was published")
    round_trip, note = read_state(json.dumps(doc))
    check(note is None and round_trip["polygons.zip"]["size"] == 4194304,
          "a state document this tool wrote reads back unchanged")
    folded, note = read_state(json.dumps(build_state(
        [obs("Polygons.ZIP", 4194304, JUL_EPOCH)])))
    check(classify(expects("polygons.zip"),
                   [obs("polygons.zip", 4194304, JUL_EPOCH)],
                   folded).state == UNCHANGED,
          "a member recorded under a different spelling of its name is still "
          "recognised")
    check(build_state(both)["recorded"] > 1700000000.0,
          "a state document written with no clock given uses this host clock")

    # ---- the report
    lines, code = describe(back)
    check(lines[0] == "feedstamp: REGRESSED" and lines[-1] == "VERDICT: REFUSE",
          "a refusal opens with the state and closes with the verdict")
    check(code == 1 and len(lines) == 4,
          "and every reason sits between them")
    lines, code = describe(same, allow_unchanged=True)
    check(lines[-1] == "VERDICT: GO" and "--allow-unchanged is set" in lines[-2],
          "an allowed UNCHANGED says which flag allowed it")
    report = json.loads(json_report(moved_on, observed=both))
    check(report["state"] == FRESH and report["verdict"] == "GO"
          and report["exit_code"] == 0,
          "the JSON report carries the state, the verdict and the exit code")
    check(report["observed"][0]["mtime"] == "2026-07-15 12:00:00Z",
          "and prints its timestamps in UTC as well")

    # ---- argument parsing
    args = _parse(["--dir", "staging", "--expect", "polygons.zip:1000000",
                   "--expect", "attributes.zip", "--state", "s.json"])
    check(args.dir == "staging" and args.state == "s.json",
          "the directory and the state file come through as given")
    check(args.expect == ["polygons.zip:1000000", "attributes.zip"],
          "--expect is repeatable and keeps the order it was given in")
    check(args.record is False and args.allow_unchanged is False
          and args.json is False,
          "recording, allowing UNCHANGED and JSON output are all off by default")
    check(args.min_quiet == 0 and args.stamp == [],
          "so is the quiet period, and no stamps are supplied")
    args = _parse(["--stamp", "polygons.zip:1:2", "--expect", "polygons.zip",
                   "--min-quiet", "300", "--record", "--allow-unchanged",
                   "--json", "--state", "s.json"])
    check(args.stamp == ["polygons.zip:1:2"] and args.min_quiet == 300,
          "a stamp and a quiet period are read from the command line")
    check(args.record and args.allow_unchanged and args.json,
          "and the three switches turn on together")
    check(_parse(["--self-test"]).self_test is True,
          "--self-test needs nothing else on the command line")

    # ---- a unique prefix of the write flag is refused, not read as --record
    refused = False
    err_saved = sys.stderr
    sys.stderr = io.StringIO()
    try:
        _parse(["--rec", "--expect", "polygons.zip"])
    except SystemExit:
        refused = True
    finally:
        sys.stderr = err_saved
    check(refused, "a unique prefix of --record is refused, not read as "
          "--record  <-- pinned defect")

    # ---- the shortest unique prefix of the write flag is refused too
    refused = False
    err_saved = sys.stderr
    sys.stderr = io.StringIO()
    try:
        _parse(["--r", "--expect", "polygons.zip"])
    except SystemExit:
        refused = True
    finally:
        sys.stderr = err_saved
    check(refused, "a one letter prefix of --record is refused, not read as "
          "--record  <-- pinned defect")

    # ---- the io layer and the cli, in a temporary directory
    def run_main(argv):
        """Run the cli, giving back the exit code and everything it printed."""
        buf = io.StringIO()
        out, err = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = buf
        try:
            code = main(argv)
        finally:
            sys.stdout, sys.stderr = out, err
        return code, buf.getvalue()

    tmp = tempfile.mkdtemp(prefix="feedstamp-selftest-")
    try:
        def deliver(name, size, mtime):
            """Write one synthetic delivery member and stamp it."""
            path = os.path.join(tmp, name)
            handle = open(path, "wb")
            try:
                handle.write(b"x" * size)
            finally:
                handle.close()
            os.utime(path, (mtime, mtime))
            return path

        state_path = os.path.join(tmp, "feedstamp-state.json")
        monday = time.time() - 3 * DAY
        deliver("polygons.zip", 4096, monday)
        deliver("attributes.zip", 2048, monday)
        manifest = expects("polygons.zip:1000", "attributes.zip:1000")

        seen = observe_directory(tmp, manifest)
        check(len(seen) == 2 and seen[0].size == 4096,
              "the directory probe reports the real size on disk")
        check(abs(seen[0].mtime - monday) < MTIME_EPSILON,
              "and the modification time the filesystem kept")
        check(len(observe_directory(tmp, expects("nothing-here.zip"))) == 0,
              "a member that is not on disk is simply not observed")

        base = ["--dir", tmp, "--expect", "polygons.zip:1000",
                "--expect", "attributes.zip:1000", "--state", state_path]
        code, out = run_main(base)
        check(code == 0 and "FIRST_RUN" in out and "VERDICT: GO" in out,
              "the first run against an empty state file is allowed")
        check(not os.path.exists(state_path),
              "and a check run writes no state file at all  <-- pinned defect")

        code, out = run_main(base + ["--record"])
        check(code == 0 and os.path.isfile(state_path),
              "--record writes the state file after the publish succeeded")
        check(not os.path.exists(state_path + ".tmp"),
              "and leaves no half written temporary file behind")
        written = json.loads(open(state_path).read())
        check(sorted(written["members"]) == ["attributes.zip", "polygons.zip"],
              "the state file records every member of the delivery")

        code, out = run_main(base)
        check(code == 1 and "UNCHANGED" in out and "VERDICT: REFUSE" in out,
              "the same two files the next night are refused  <-- pinned defect")
        code, out = run_main(base + ["--allow-unchanged"])
        check(code == 0 and "VERDICT: GO" in out,
              "unless the operator has said an unmoved feed is acceptable")

        before = open(state_path).read()
        deliver("polygons.zip", 4096, monday - 7 * DAY)
        code, out = run_main(base + ["--record"])
        check(code == 1 and "REGRESSED" in out,
              "a member restored from last week is refused")
        check(open(state_path).read() == before,
              "and --record writes nothing after a refusal  <-- pinned defect")
        check("state not recorded" in out,
              "the run says so rather than failing silently")

        deliver("polygons.zip", 9000, monday + DAY)
        code, out = run_main(base + ["--record"])
        check(code == 0 and "FRESH" in out,
              "a bigger file stamped a day later is a new delivery")
        check(json.loads(open(state_path).read())["members"]
              ["polygons.zip"]["size"] == 9000,
              "and the new size is what the next run compares against")

        deliver("attributes.zip", 0, monday + DAY)
        code, out = run_main(base)
        check(code == 1 and "INCOMPLETE" in out and "zero bytes" in out,
              "a member truncated to nothing is refused as incomplete")

        code, out = run_main(base + ["--json"])
        report = json.loads(out)
        check(report["state"] == INCOMPLETE and report["verdict"] == "REFUSE",
              "--json prints one object a caller can parse")
        before = open(state_path).read()
        code, out = run_main(base + ["--json", "--record"])
        check(code == 1 and json.loads(out)["verdict"] == "REFUSE"
              and open(state_path).read() == before,
              "--json and --record together still record nothing after a "
              "refusal, and print nothing but the object")

        deliver("attributes.zip", 2048, monday + 2 * DAY)
        code, out = run_main(base + ["--json", "--record"])
        check(code == 0 and json.loads(out)["state"] == FRESH,
              "and the object stays parseable on the run that does record")
        check(json.loads(open(state_path).read())["members"]
              ["attributes.zip"]["size"] == 2048,
              "which wrote the new delivery into the state file")

        deliver("attributes.zip", 2048, time.time())
        code, out = run_main(base + ["--min-quiet", "600"])
        check(code == 1 and "quiet period" in out,
              "a member written seconds ago is still uploading, so refused")

        stamp_state = os.path.join(tmp, "stamp-state.json")
        save_state(stamp_state,
                   build_state([Observation("polygons.zip", 9000, JAN_EPOCH)]))
        code, out = run_main(["--stamp", "polygons.zip:9000:" + JUL_MDTM,
                              "--expect", "polygons.zip:1000",
                              "--state", stamp_state])
        check(code == 0 and "FRESH" in out,
              "a stamp read from an FTP server decides without a download")
        check("2026-07-15" in out,
              "and the MDTM stamp it was given is reported back in UTC")

        # ---- usage errors, which must not read as a clean pass
        check(run_main(["--dir", tmp])[0] == 64,
              "a run with no --expect is a usage error, not an empty pass")
        check(run_main(["--expect", "polygons.zip"])[0] == 64,
              "so is a run with neither --dir nor --stamp")
        check(run_main(["--dir", tmp, "--stamp", "a:1:2",
                        "--expect", "a"])[0] == 64,
              "--dir and --stamp together is a usage error, not a merge")
        check(run_main(["--dir", os.path.join(tmp, "gone"),
                        "--expect", "polygons.zip"])[0] == 64,
              "a staging directory that is not there is a usage error"
              "  <-- pinned defect")
        check(run_main(["--dir", tmp, "--expect", "polygons.zip",
                        "--record"])[0] == 64,
              "--record with no --state has nothing to write and says so")
        check(run_main(["--dir", tmp, "--expect", "  "])[0] == 64,
              "an unreadable --expect value is a usage error")
        check(run_main(["--stamp", "polygons.zip:big:1",
                        "--expect", "polygons.zip"])[0] == 64,
              "an unreadable --stamp value is a usage error")
        code, out = run_main(["--dir", tmp, "--expect", "polygons.zip:1000",
                              "--state", os.path.join(tmp, "gone", "s.json"),
                              "--record"])
        check(code == 2, "a state file that cannot be written exits 2")

        code, out = run_main(["--dir", tmp, "--expect", "polygons.zip:1000",
                              "--state", os.path.join(tmp, "not-json.txt")])
        check(code == 0 and "FIRST_RUN" in out,
              "an unreadable state file falls back to a first run, so the "
              "pipeline is not blocked by its own bookkeeping")

        # ---- the harness itself can go red
        def harness_goes_red():
            """Drive both helpers at a case that must fail, without the noise.

            An assertion that cannot fail raises the count and proves nothing,
            so the two helpers that decide the count are themselves tested.
            """
            quiet = io.StringIO()
            out = sys.stdout
            sys.stdout = quiet
            try:
                check(False, "a deliberately false case")
                raises(lambda: None, "a deliberately non-raising case")
                raises(lambda: [][0], "a deliberately wrong exception")
            finally:
                sys.stdout = out
            recorded = [f for f in failed if "deliberately" in f]
            failed[:] = [f for f in failed if "deliberately" not in f]
            return (len(recorded) == 3
                    and quiet.getvalue().count("FAIL") == 3
                    and "wrong exception" in recorded[2])

        check(harness_goes_red(),
              "check() records a failure, and raises() records both a case "
              "that did not raise and a case that raised the wrong thing"
              "  <-- pinned defect")
        lines, code = summarise(3, ["a case that went red"])
        check(code == 1 and lines == ["4 assertions, 1 failed",
                                      "  FAILED: a case that went red"],
              "a run with one failure in it names that failure and exits 1")
        check(summarise(4, []) == (["4 assertions, 0 failed"], 0),
              "and a clean run reports the count and exits 0")
        check(passed[0] > 100,
              "and the count this run reports is a whole run, not a truncated "
              "one")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    check(not os.path.isdir(tmp),
          "the self-test leaves no temporary directory behind")

    print("-" * 68)
    lines, code = summarise(passed[0], failed)
    for line in lines:
        print(line)
    return code


# ----------------------------------------------------------------------- cli

def _parse(argv):
    ap = argparse.ArgumentParser(
        prog="feedstamp.py",
        description="Refuse to run the pipeline when the delivery is not a "
                    "new, complete one.",
        epilog="The state file is written only by --record, and only after a "
               "verdict of GO.",
        allow_abbrev=False,
    )
    ap.add_argument("--dir", dest="dir", metavar="PATH",
                    help="staging directory holding the delivery")
    ap.add_argument("--stamp", action="append", default=[],
                    metavar="NAME:SIZE:MTIME",
                    help="one observation from a probe made elsewhere, such "
                         "as an FTP SIZE and MDTM pair. Repeatable.")
    ap.add_argument("--expect", action="append", default=[],
                    metavar="NAME[:MINBYTES]",
                    help="a member the delivery must contain, and the size it "
                         "must clear. Repeatable. Required.")
    ap.add_argument("--state", metavar="PATH",
                    help="state file written after the last successful publish")
    ap.add_argument("--min-quiet", dest="min_quiet", type=int,
                    default=os.environ.get("FEEDSTAMP_MIN_QUIET", 0),
                    help="refuse a member modified within this many seconds, "
                         "because it may still be uploading (default 0, off). "
                         "Env: FEEDSTAMP_MIN_QUIET")
    ap.add_argument("--allow-unchanged", dest="allow_unchanged",
                    action="store_true",
                    help="treat a delivery that did not move as acceptable")
    ap.add_argument("--record", action="store_true",
                    help="write the state file. Run this after the publish "
                         "succeeded, never before it.")
    ap.add_argument("--json", action="store_true",
                    help="print one JSON object instead of the report")
    ap.add_argument("--self-test", dest="self_test", action="store_true",
                    help="run the assertions and exit")
    # argparse runs type= over a default that is still a string, so an
    # unreadable FEEDSTAMP_MIN_QUIET is a usage error rather than a traceback.
    return ap.parse_args(argv)


def main(argv=None):
    args = _parse(sys.argv[1:] if argv is None else argv)

    if args.self_test:
        return self_test()

    if not args.expect:
        print("error: pass --expect NAME for every member the delivery must "
              "contain. Use --self-test to check the tool without one.",
              file=sys.stderr)
        return 64
    try:
        expects = [parse_expect(text) for text in args.expect]
        stamps = [parse_stamp(text) for text in args.stamp]
    except ValueError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 64

    if args.dir and stamps:
        print("error: pass --dir or --stamp, not both. They are two answers "
              "to the same question.", file=sys.stderr)
        return 64
    if args.dir:
        if not os.path.isdir(args.dir):
            # A staging directory that is not there would otherwise report
            # every member missing, which is the right verdict for the wrong
            # reason and hides a typo in a scheduled task for months.
            print("error: no such staging directory: %s" % args.dir,
                  file=sys.stderr)
            return 64
        observed = observe_directory(args.dir, expects)
    elif stamps:
        observed = stamps
    else:
        print("error: pass --dir PATH or --stamp NAME:SIZE:MTIME.",
              file=sys.stderr)
        return 64
    if args.record and not args.state:
        print("error: --record needs --state PATH to write to.",
              file=sys.stderr)
        return 64

    prior, note = load_state(args.state)
    result = classify(expects, observed, prior, now=time.time(),
                      min_quiet=args.min_quiet, prior_note=note)
    lines, code = describe(result, args.allow_unchanged)
    allowed = code == 0

    if args.json:
        print(json_report(result, args.allow_unchanged, observed))
    else:
        for line in lines:
            print(line)

    if args.record:
        if not allowed:
            if not args.json:
                print("state not recorded: the delivery was refused")
            return code
        try:
            save_state(args.state, build_state(observed))
        except OSError as exc:
            print("error: the state file could not be written (%s)" % exc,
                  file=sys.stderr)
            return 2
        if not args.json:
            print("state recorded in %s" % args.state)
    return code


if __name__ == "__main__":
    sys.exit(main())
