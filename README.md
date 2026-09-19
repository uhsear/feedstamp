# feedstamp

Refuse to run the pipeline when the delivery is not a new, complete one. Decides before the
download, before the load, and before arcpy is imported.

The vendor's export job dies on a Tuesday and leaves Friday's zip sitting on the server. The
nightly task downloads it, unzips it, stages it and republishes it. Nothing fails. The row counts
match, because the data is internally consistent: it is four days old. The scheduler reports a
clean run, correctly. The load guard allows the swap, correctly. Every control in the estate
watches the job, and none of them watches the payload.

The other half of the same failure arrives on time. The vendor is still uploading at 02:00 when
the task starts. The file is there and its timestamp is today's, but it is 900 kB of a 4.2 MB
export. An unzip of a truncated archive succeeds often enough to matter. What lands in production
is a real subset of the county, with no error anywhere in the log.

```
$ python feedstamp.py --self-test
feedstamp self-test: no network, no arcpy, no database
--------------------------------------------------------------------
PASS  a july MDTM stamp reads as UTC, not as local time  <-- pinned defect
PASS  a january MDTM stamp reads as UTC too  <-- pinned defect
PASS  the two halves of the year are 181 days apart, with no summer time step in between  <-- pinned defect
PASS  the local-time reading of those same two stamps loses this host's summer time shift of 3600 second(s)  <-- pinned defect
...
PASS  a NaN timestamp is refused at the boundary, because every comparison against it is False and the verdict would be GO  <-- pinned defect
PASS  so is an infinite one  <-- pinned defect
PASS  a --stamp carrying NaN is refused with it  <-- pinned defect
PASS  the ceiling is the last epoch second both Windows and Linux can print  <-- pinned defect
...
PASS  a missing manifest member is named, not just counted  <-- pinned defect
PASS  a zero byte member is INCOMPLETE against a full prior state  <-- pinned defect
PASS  a zero byte member is INCOMPLETE even when its floor is zero  <-- pinned defect
PASS  the same member five minutes later is complete
PASS  a member whose on-disk stamp is past the year 3000 is INCOMPLETE, not a traceback inside the report  <-- pinned defect
PASS  and a NaN one is INCOMPLETE rather than FRESH  <-- pinned defect
...
PASS  a delivery stamped a week earlier than the last published one is REGRESSED  <-- pinned defect
PASS  a naive changed-since check reads that same pair as a delivery worth loading  <-- pinned defect
PASS  REGRESSED refuses even with --allow-unchanged set  <-- pinned defect
PASS  three seconds back is past the rounding tolerance and refuses
PASS  incompleteness is decided first, so a broken old delivery is reported as broken, not as old
PASS  a stamp exactly two seconds on is FAT rounding, so still UNCHANGED
PASS  and one hundredth of a second past the tolerance is a new delivery
PASS  a size that changed with the timestamp unchanged is FRESH  <-- pinned defect
PASS  a corrupt state file returns a reason instead of raising  <-- pinned defect
PASS  and the run falls back to FIRST_RUN carrying that reason  <-- pinned defect
PASS  json writes a NaN stamp as a bare token, so a state file can hold one  <-- pinned defect
PASS  and reading it back is damage, not a stamp  <-- pinned defect
...
PASS  and a check run writes no state file at all  <-- pinned defect
PASS  the same two files the next night are refused  <-- pinned defect
PASS  and --record writes nothing after a refusal  <-- pinned defect
PASS  a staging directory that is not there is a usage error  <-- pinned defect
PASS  check() records a failure, and raises() records both a case that did not raise and a case that raised the wrong thing  <-- pinned defect
PASS  the self-test leaves no temporary directory behind
--------------------------------------------------------------------
150 assertions, 0 failed
```

## Requirements

Python 3.9 or newer. Nothing to install, no `arcpy`, no third-party package, no network
connection and no database. The whole tool is one file, and `--self-test` runs anywhere Python
does.

It has been run green on Windows with Python 3.13.2, on ArcGIS Pro's Python 3.13.7, and on Ubuntu
with Python 3.12.3. All three report the same 150 assertions.

```
git clone https://github.com/uhsear/feedstamp.git
```

## Quick start

```
python feedstamp.py --self-test
python feedstamp.py --dir staging --expect polygons.zip:1000000
```

## Usage

feedstamp is called twice per run, and the second call is the one that writes. Check before the
pipeline starts, and record only after the publish has succeeded.

```
python feedstamp.py --dir staging --expect polygons.zip:1000000 \
    --expect attributes.zip:1000000 --state feed.json   || exit 1

...download, unzip, stage, load, publish...

python feedstamp.py --dir staging --expect polygons.zip:1000000 \
    --expect attributes.zip:1000000 --state feed.json --record
```

A real sequence, run against a synthetic two-member delivery:

```
$ python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect attributes.zip:1000000 --state feed.json --record
feedstamp: FIRST_RUN
    no prior state file, so there is nothing to compare this delivery against
VERDICT: GO
state recorded in feed.json
(exit 0)

$ python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect attributes.zip:1000000 --state feed.json
feedstamp: UNCHANGED
    every expected member has the size and timestamp recorded after the last publish
VERDICT: REFUSE
(exit 1)

$ python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect attributes.zip:1000000 --state feed.json --record
feedstamp: REGRESSED
    polygons.zip is stamped 2026-09-09 00:22:04Z, older than the 2026-09-16 00:22:00Z recorded after the last publish
VERDICT: REFUSE
state not recorded: the delivery was refused
(exit 1)

$ python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect attributes.zip:1000000 --state feed.json --min-quiet 300
feedstamp: INCOMPLETE
    polygons.zip was written 0 second(s) ago, inside the 300 second quiet period
VERDICT: REFUSE
(exit 1)

$ python feedstamp.py --dir staging --expect polygons.zip:1000000 --expect attributes.zip:1000000 --state feed.json --record
feedstamp: FRESH
    polygons.zip is 4300000 bytes, was 4200000 bytes
VERDICT: GO
state recorded in feed.json
(exit 0)
```

To decide before downloading anything, hand it what your own probe found. This is the whole
integration, and it is why the tool opens no socket of its own:

```python
import ftplib, subprocess
ftp = ftplib.FTP_TLS(host); ftp.login(user, password); ftp.prot_p()
stamp = "%s:%d:%s" % (name, ftp.size(name), ftp.sendcmd("MDTM " + name))
code = subprocess.call(["python", "feedstamp.py", "--stamp", stamp,
                        "--expect", name, "--state", "feed.json"])
```

`--stamp` accepts the whole `213 20260918041500` reply line, the bare digits, or epoch seconds.
Fourteen digits are read as an MDTM timestamp in UTC; anything else is epoch seconds.

```
$ python feedstamp.py --stamp polygons.zip:4300000:20260918041500 --expect polygons.zip:1000000 --state feed.json
feedstamp: REGRESSED
    polygons.zip is stamped 2026-09-18 04:15:00Z, older than the 2026-09-19 00:22:07Z recorded after the last publish
VERDICT: REFUSE
(exit 1)
```

| Flag | Default | What it does |
|---|---|---|
| `--expect` | none | A member the delivery must contain, as `NAME` or `NAME:MINBYTES`. Repeatable. Required. |
| `--dir` | none | Staging directory to stat the expected members in. |
| `--stamp` | none | One observation from a probe made elsewhere, as `NAME:SIZE:MTIME`. Repeatable. Use this or `--dir`, not both. |
| `--state` | none | State file written after the last successful publish. Without it every run is a first run. |
| `--min-quiet` | `0` | Refuse a member modified within this many seconds, because it may still be uploading. Env: `FEEDSTAMP_MIN_QUIET` |
| `--allow-unchanged` | off | Treat a delivery that did not move as acceptable. |
| `--record` | off | Write the state file. Nothing is written without it, and nothing is written after a refusal. |
| `--json` | off | Print one JSON object instead of the report. |
| `--self-test` | off | Run the assertions and exit. |

## What it checks

Completeness is decided first and answers on its own, because a half uploaded file compared
against last week's stamp only produces a second, wrong answer.

| State | What it means | Verdict |
|---|---|---|
| `INCOMPLETE` | A member is missing, is zero bytes, is under its `--expect` floor, carries a timestamp that is not a readable date, or was modified inside `--min-quiet`. | REFUSE, always |
| `REGRESSED` | A member is stamped earlier than the copy recorded after the last publish. | REFUSE, always |
| `FIRST_RUN` | There is no state file, or the state file cannot be trusted. | GO |
| `UNCHANGED` | Every member matches the size and timestamp recorded after the last publish. | REFUSE, or GO with `--allow-unchanged` |
| `FRESH` | A size or a timestamp moved forward, or a member is new to the manifest. | GO |

`UNCHANGED` is the only verdict that is an operator policy rather than a fact. A feed rebuilt
every night is broken when it does not move. A feed that changes only when someone changes it is
not. `REGRESSED` ignores `--allow-unchanged`, because a delivery going backwards is never the
thing the pipeline was waiting for.

## Where this sits in the chain

Three guards, three different moments, and this one is the cheapest because it decides before
anything is read.

- **feedstamp guards the delivery, before the load.** Is this file new, and is it whole.
- **[safe-republish](https://github.com/uhsear/safe-republish) guards the load.** Is the staged
  replacement plausible against the live row count. It is blind by construction to a staged table
  that is complete, plausible and a week old, because every count matches.
- **[jobharness](https://github.com/uhsear/jobharness) guards the run.** Logging, retry and
  resume for the steps of this execution. It will faithfully retry the download of yesterday's
  file.

feedstamp does no sidecar checking. A shapefile staged without its `.prj` is
[fcload](https://github.com/uhsear/fcload)'s refusal at load time and
[gdbfence](https://github.com/uhsear/gdbfence)'s refusal at commit time. Reimplementing it here
would only give you two tools that disagree.

## Why the obvious version is wrong

The obvious version is one line, remembered between runs:

```python
if os.path.getmtime(path) != last_seen:      # wrong
    run_pipeline()
```

It fails in both directions. A vendor export that dies and restores an older copy moves the
timestamp **backwards**. The `!=` reads that as a new delivery, which is the failure this tool
exists to catch. A file that is still uploading has a timestamp that already moved. Its size will
move again in four minutes, so the same check starts the pipeline on a partial file. Replacing
`!=` with `>` fixes the first case and hides it. The run then reports nothing wrong, and quietly
never runs again until somebody notices the map is stale.

The second trap is the timezone, and it only appears in production. FTP's `MDTM` reply is UTC by
protocol and carries no zone (RFC 3659 section 3). Both natural readings of it are local:

```python
time.mktime(time.strptime(text, "%Y%m%d%H%M%S"))                 # wrong
datetime.datetime.strptime(text, "%Y%m%d%H%M%S").timestamp()     # wrong
```

On a host that keeps summer time, those readings of the same instant sit an hour apart across the
year. `calendar.timegm` is the reading with no zone in it. The self-test pins both halves of the
year against fixed constants, and measures the shift the local reading loses on the host running
it: 3600 seconds on the Eastern workstation above, and 0 on a UTC server.

## Is this already solved

Not in this estate, and the absence is measured rather than argued.

Search the four script trees the author owns for `MDTM`, `Last-Modified`, `getmtime`, `st_mtime`,
`ETag` or `hashlib`. The hits land in one tool only, and there `st_mtime` ages out that tool's own
log files. None of the six scripts that retrieve a vendor delivery asks whether the file changed
since the last run.

The completeness half exists, hand-written, exactly once: a function that checks two extracted
files are present and that each exceeds 1,000,000 bytes. Two more scripts take the first `.shp`
they find in the extract directory and check nothing about it. The remaining three go straight to
a hard-coded path and check nothing at all. This tool generalises the one good version and adds
the freshness half that nothing there has.

Across the author's 37 published tools, `MDTM`, `Last-Modified`, `If-Modified` and `ETag` return
zero files.

`wget --timestamping` and `curl -z` answer the transfer half of this question. Neither of them
refuses anything. They skip the transfer when the remote copy is not newer, and treat that as a
successful run. The pipeline behind them then proceeds on whatever is already on disk. The
decision this tool makes is the one those flags exist to avoid making.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | The delivery may be consumed: `FRESH`, `FIRST_RUN`, or `UNCHANGED` with `--allow-unchanged`. |
| 1 | Refused: `INCOMPLETE`, `REGRESSED`, or `UNCHANGED`. |
| 2 | The state file could not be written. The verdict was already printed. |
| 64 | Usage error: no `--expect`, neither `--dir` nor `--stamp`, both of them, a staging directory that does not exist, `--record` without `--state`, or an unreadable flag value. |

A staging directory that does not exist is a usage error rather than a delivery with every member
missing. Both refuse, but only one of them names the typo, and a scheduled task can carry that
typo for months.

An unreadable `FEEDSTAMP_MIN_QUIET` is argparse's own error and exits 2.

## Limits

- **It makes no network connection.** `--dir` stats a staging directory and `--stamp` takes what
  your own probe found. Shipping an FTP client here would mean shipping socket code the offline
  self-test cannot exercise, and that is the code that breaks.
- **Size and timestamp only, never content.** A vendor who reships last week's rows inside a file
  written today is FRESH here and wrong. That is `safe-republish`'s question, after the load.
- **The state file describes the feed.** It holds delivered file names, sizes and timestamps.
  Keep it beside the staging directory, not in git; the shipped `.gitignore` has a rule for it.
- **A state file that cannot be read allows the run.** It becomes `FIRST_RUN` with the reason
  printed, rather than an exception. Blocking a pipeline over this tool's own bookkeeping file
  would be the worse outcome. It does mean that a deleted state file silently permits one stale
  delivery.
- **A timestamp must be a real epoch second between 1970 and the year 3000.** NaN, infinity and
  a date past that ceiling are refused. `--stamp` calls it a usage error, the state file counts
  it as damage, and `os.stat` makes the member `INCOMPLETE`. NaN is the one that matters. Every
  comparison against it is false, so an unguarded NaN reads as `FRESH` and the pipeline starts.
  The ceiling is where Windows' `time.gmtime` stops and Linux's does not. A stamp past it would
  print on one host and raise on the other.
- **Member names are matched by their last path component, without case.** `ROADS.ZIP` and
  `roads.zip` are one member. On a case-sensitive server that genuinely delivers both, this is
  wrong.
- **Two seconds of tolerance on every timestamp comparison**, because FAT and exFAT store
  modification times to the nearest two seconds. A vendor who republishes within two seconds of
  the previous stamp, at the same size, reads as `UNCHANGED`.
- **A member stamped in the future is accepted as `FRESH`** unless `--min-quiet` is on, and the
  run after it reads that member as `REGRESSED`. Use `--min-quiet` on a feed whose server clock
  you do not trust.
- **`--min-quiet` is off by default**, so the check is deterministic: the same delivery gets the
  same verdict whenever you run it. Turning it on makes the verdict depend on the clock.
- **It has no opinion about how many members a delivery has.** Only the members named by
  `--expect` are looked at. A member the vendor added is invisible until somebody adds it to the
  manifest.

## Contributing

Open an issue or pull request on GitHub.

## Author

Built by [Asir Khan](https://www.linkedin.com/in/asir-khan-310317264/).

## License

MIT.

## Related

Other single-file tools in this portfolio that pair with this one:

- [safe-republish](https://github.com/uhsear/safe-republish) - the same pipeline, one step later: refuses the truncate when the staged replacement is implausible
- [jobharness](https://github.com/uhsear/jobharness) - logging, retry, resume and an FTPS client for the unattended job that calls this one
- [fcload](https://github.com/uhsear/fcload) - loads the delivery this tool approved, and refuses the imports that corrupt silently
