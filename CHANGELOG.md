# Change Log

## [v0.9.14.6](https://github.com/Yelp/Tron/tree/v0.9.14.6) (2019-07-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.5...v0.9.14.6)

**Merged pull requests:**

- Make string sorting only in ActionRun displaying. [\#688](https://github.com/Yelp/Tron/pull/688) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.14.5](https://github.com/Yelp/Tron/tree/v0.9.14.5) (2019-07-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.4...v0.9.14.5)

**Merged pull requests:**

- Fix sorting for DisplayActionRuns when fields are None [\#687](https://github.com/Yelp/Tron/pull/687) ([solarkennedy](https://github.com/solarkennedy))
- Use newlines for the stderr array in check\_tron\_jobs [\#686](https://github.com/Yelp/Tron/pull/686) ([solarkennedy](https://github.com/solarkennedy))
- Use starting update from Mesos [\#684](https://github.com/Yelp/Tron/pull/684) ([qui](https://github.com/qui))
- Determine that runner is not running anymore if pid is none for recover\_batch [\#683](https://github.com/Yelp/Tron/pull/683) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.14.4](https://github.com/Yelp/Tron/tree/v0.9.14.4) (2019-07-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.3...v0.9.14.4)

**Merged pull requests:**

- Fix stderr output on check\_tron\_jobs [\#682](https://github.com/Yelp/Tron/pull/682) ([solarkennedy](https://github.com/solarkennedy))
- Sort action\_runs by start\_time in tronview [\#680](https://github.com/Yelp/Tron/pull/680) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.14.3](https://github.com/Yelp/Tron/tree/v0.9.14.3) (2019-07-11)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.2...v0.9.14.3)

**Merged pull requests:**

- Use last run time to get next run time before scheduling [\#681](https://github.com/Yelp/Tron/pull/681) ([qui](https://github.com/qui))

## [v0.9.14.2](https://github.com/Yelp/Tron/tree/v0.9.14.2) (2019-07-05)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.1...v0.9.14.2)

**Merged pull requests:**

- Bump parso [\#679](https://github.com/Yelp/Tron/pull/679) ([keymone](https://github.com/keymone))
- Improve logging for deferred errors and apply timeout config to trans… [\#678](https://github.com/Yelp/Tron/pull/678) ([qui](https://github.com/qui))
- Put stderr at the front of the check\_tron\_jobs notification [\#677](https://github.com/Yelp/Tron/pull/677) ([solarkennedy](https://github.com/solarkennedy))
- Bump Twisted and Jinja [\#676](https://github.com/Yelp/Tron/pull/676) ([keymone](https://github.com/keymone))
- clean up alerts and ddbbackup [\#675](https://github.com/Yelp/Tron/pull/675) ([EmanekaT](https://github.com/EmanekaT))
- Preserve last run's scheduled time when reconfiguring [\#674](https://github.com/Yelp/Tron/pull/674) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.14.1](https://github.com/Yelp/Tron/tree/v0.9.14.1) (2019-06-03)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.14.0...v0.9.14.1)

**Merged pull requests:**

- Add tron\_stateless\_alert for dynamodb [\#673](https://github.com/Yelp/Tron/pull/673) ([EmanekaT](https://github.com/EmanekaT))
- Set up periodic backup for DynamoDB [\#671](https://github.com/Yelp/Tron/pull/671) ([EmanekaT](https://github.com/EmanekaT))
- fixed make dev [\#670](https://github.com/Yelp/Tron/pull/670) ([EmanekaT](https://github.com/EmanekaT))
- Build for bionic [\#663](https://github.com/Yelp/Tron/pull/663) ([jvperrin](https://github.com/jvperrin))
- Bump dependencies [\#661](https://github.com/Yelp/Tron/pull/661) ([keymone](https://github.com/keymone))

## [v0.9.14.0](https://github.com/Yelp/Tron/tree/v0.9.14.0) (2019-05-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.13.4...v0.9.14.0)

**Merged pull requests:**

- Runs with triggers should start in scheduled state [\#668](https://github.com/Yelp/Tron/pull/668) ([qui](https://github.com/qui))

## [v0.9.13.4](https://github.com/Yelp/Tron/tree/v0.9.13.4) (2019-05-14)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.13.3...v0.9.13.4)

**Merged pull requests:**

- Revert "Revert "removed mirror store"" [\#667](https://github.com/Yelp/Tron/pull/667) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.13.3](https://github.com/Yelp/Tron/tree/v0.9.13.3) (2019-05-14)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.13.2...v0.9.13.3)

**Merged pull requests:**

- Make recovery batch script also check if action runner suddenly goes away [\#666](https://github.com/Yelp/Tron/pull/666) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Revert "removed mirror store" [\#665](https://github.com/Yelp/Tron/pull/665) ([EmanekaT](https://github.com/EmanekaT))
- Prevent job runs from incorrectly getting stuck in WAITING [\#664](https://github.com/Yelp/Tron/pull/664) ([qui](https://github.com/qui))
- Split the tab completion file [\#662](https://github.com/Yelp/Tron/pull/662) ([tzhu-yelp](https://github.com/tzhu-yelp))
- Place the bash completion script in the completion dir [\#660](https://github.com/Yelp/Tron/pull/660) ([tzhu-yelp](https://github.com/tzhu-yelp))
- Added lots more task logging for unknown events [\#659](https://github.com/Yelp/Tron/pull/659) ([solarkennedy](https://github.com/solarkennedy))
- removed mirror store [\#654](https://github.com/Yelp/Tron/pull/654) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.13.2](https://github.com/Yelp/Tron/tree/v0.9.13.2) (2019-05-02)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.13.1...v0.9.13.2)

**Merged pull requests:**

- fix guess\_realert when next\_run is the same as previous\_run [\#658](https://github.com/Yelp/Tron/pull/658) ([EmanekaT](https://github.com/EmanekaT))
- Some doc updates [\#657](https://github.com/Yelp/Tron/pull/657) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.13.1](https://github.com/Yelp/Tron/tree/v0.9.13.1) (2019-04-30)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.13.0...v0.9.13.1)

**Merged pull requests:**

- Changed dynamodb partition index to int so it is sorted correctly [\#656](https://github.com/Yelp/Tron/pull/656) ([EmanekaT](https://github.com/EmanekaT))
- Prevent a connection error in the recovery action from failing the action [\#655](https://github.com/Yelp/Tron/pull/655) ([qui](https://github.com/qui))
- Return 200 if no code is passed and the response is not a dict [\#653](https://github.com/Yelp/Tron/pull/653) ([qui](https://github.com/qui))
- Fixed guess\_realert\_every [\#652](https://github.com/Yelp/Tron/pull/652) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.13.0](https://github.com/Yelp/Tron/tree/v0.9.13.0) (2019-04-22)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.6...v0.9.13.0)

**Merged pull requests:**

- reload old config if new config fails [\#651](https://github.com/Yelp/Tron/pull/651) ([drmorr0](https://github.com/drmorr0))
- Use correct parser for months in cron [\#650](https://github.com/Yelp/Tron/pull/650) ([qui](https://github.com/qui))
- duration field added to tronweb [\#649](https://github.com/Yelp/Tron/pull/649) ([drmorr0](https://github.com/drmorr0))
- Log state changes [\#646](https://github.com/Yelp/Tron/pull/646) ([qui](https://github.com/qui))

## [v0.9.12.6](https://github.com/Yelp/Tron/tree/v0.9.12.6) (2019-04-19)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.5...v0.9.12.6)

**Merged pull requests:**

-  Removed interval scheduler [\#648](https://github.com/Yelp/Tron/pull/648) ([EmanekaT](https://github.com/EmanekaT))
- "Remove" interval scheduler from documentation [\#647](https://github.com/Yelp/Tron/pull/647) ([EmanekaT](https://github.com/EmanekaT))
- Workaround for trond segfaults [\#645](https://github.com/Yelp/Tron/pull/645) ([vkhromov](https://github.com/vkhromov))
- Check for failed or unknown action if a run is waiting [\#644](https://github.com/Yelp/Tron/pull/644) ([qui](https://github.com/qui))

## [v0.9.12.5](https://github.com/Yelp/Tron/tree/v0.9.12.5) (2019-04-15)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.4...v0.9.12.5)

**Merged pull requests:**

- alert on failed actions before stuck runs [\#642](https://github.com/Yelp/Tron/pull/642) ([drmorr0](https://github.com/drmorr0))

## [v0.9.12.4](https://github.com/Yelp/Tron/tree/v0.9.12.4) (2019-04-10)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.3...v0.9.12.4)

**Merged pull requests:**

- check\_tron\_jobs no longer alerts if job is not scheduled [\#641](https://github.com/Yelp/Tron/pull/641) ([acoover](https://github.com/acoover))
- Improve dynamodb read/write speed [\#640](https://github.com/Yelp/Tron/pull/640) ([EmanekaT](https://github.com/EmanekaT))
- Make check\_tron\_jobs look at entire history by removing count arg [\#636](https://github.com/Yelp/Tron/pull/636) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Just get run state once to reduce iterating on deque [\#627](https://github.com/Yelp/Tron/pull/627) ([qui](https://github.com/qui))

## [v0.9.12.3](https://github.com/Yelp/Tron/tree/v0.9.12.3) (2019-04-03)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.2...v0.9.12.3)

**Merged pull requests:**

- changed max number of open files to 10000 [\#639](https://github.com/Yelp/Tron/pull/639) ([EmanekaT](https://github.com/EmanekaT))
- Bump taskproc to 0.1.5 to include fix that restarts driver on error [\#638](https://github.com/Yelp/Tron/pull/638) ([qui](https://github.com/qui))

## [v0.9.12.2](https://github.com/Yelp/Tron/tree/v0.9.12.2) (2019-04-02)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.1...v0.9.12.2)

**Merged pull requests:**

- Make macro\_timedelta round days if day \> a month's days [\#637](https://github.com/Yelp/Tron/pull/637) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- fixed a bug in dynamodb validation function [\#635](https://github.com/Yelp/Tron/pull/635) ([EmanekaT](https://github.com/EmanekaT))
- Handle starting state from Mesos [\#632](https://github.com/Yelp/Tron/pull/632) ([qui](https://github.com/qui))

## [v0.9.12.1](https://github.com/Yelp/Tron/tree/v0.9.12.1) (2019-03-22)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.12.0...v0.9.12.1)

**Merged pull requests:**

- fixed data validation of dynamodb migration  [\#631](https://github.com/Yelp/Tron/pull/631) ([EmanekaT](https://github.com/EmanekaT))
- Allow the tron api get response to handle lists as well as dicts. [\#630](https://github.com/Yelp/Tron/pull/630) ([solarkennedy](https://github.com/solarkennedy))
- use mesos 1.7.2 in itests [\#629](https://github.com/Yelp/Tron/pull/629) ([stug](https://github.com/stug))

## [v0.9.12.0](https://github.com/Yelp/Tron/tree/v0.9.12.0) (2019-03-13)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.11.1...v0.9.12.0)

## [v0.9.11.1](https://github.com/Yelp/Tron/tree/v0.9.11.1) (2019-03-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.11.0...v0.9.11.1)

**Merged pull requests:**

- Fixed trusty build [\#628](https://github.com/Yelp/Tron/pull/628) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.11.0](https://github.com/Yelp/Tron/tree/v0.9.11.0) (2019-03-08)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.10.0...v0.9.11.0)

**Merged pull requests:**

- Added back a trusty target to the travis matrix and not duplicate running unit tests. [\#626](https://github.com/Yelp/Tron/pull/626) ([solarkennedy](https://github.com/solarkennedy))
- Migrating from Berkley DB to DynamoDB \(TRON-638\) [\#617](https://github.com/Yelp/Tron/pull/617) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.10.0](https://github.com/Yelp/Tron/tree/v0.9.10.0) (2019-03-04)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.15...v0.9.10.0)

**Merged pull requests:**

- Waiting state if an action is waiting for normal or cross-job dependencies [\#622](https://github.com/Yelp/Tron/pull/622) ([qui](https://github.com/qui))

## [v0.9.9.15](https://github.com/Yelp/Tron/tree/v0.9.9.15) (2019-03-04)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.14...v0.9.9.15)

**Merged pull requests:**

- Add waiting state first for rollback safety [\#624](https://github.com/Yelp/Tron/pull/624) ([qui](https://github.com/qui))
- use mesos 1.7.1 in itests [\#623](https://github.com/Yelp/Tron/pull/623) ([stug](https://github.com/stug))
- Add docs for monitoring defaults [\#621](https://github.com/Yelp/Tron/pull/621) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Removed trusty support [\#620](https://github.com/Yelp/Tron/pull/620) ([solarkennedy](https://github.com/solarkennedy))
- Don't upgrade all the things when building a package [\#619](https://github.com/Yelp/Tron/pull/619) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.9.14](https://github.com/Yelp/Tron/tree/v0.9.9.14) (2019-02-08)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.13...v0.9.9.14)

**Merged pull requests:**

- Ignore duration=None for jobs waiting on external dependency [\#615](https://github.com/Yelp/Tron/pull/615) ([keymone](https://github.com/keymone))

## [v0.9.9.13](https://github.com/Yelp/Tron/tree/v0.9.9.13) (2019-02-07)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.12...v0.9.9.13)

**Merged pull requests:**

- Add tronctl version subcommand [\#612](https://github.com/Yelp/Tron/pull/612) ([keymone](https://github.com/keymone))

## [v0.9.9.12](https://github.com/Yelp/Tron/tree/v0.9.9.12) (2019-02-07)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.11...v0.9.9.12)

**Merged pull requests:**

- Fix inconsistency between JobRun state attribute and is\_\<state\> checks [\#613](https://github.com/Yelp/Tron/pull/613) ([keymone](https://github.com/keymone))

## [v0.9.9.11](https://github.com/Yelp/Tron/tree/v0.9.9.11) (2019-02-04)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.10...v0.9.9.11)

**Merged pull requests:**

- Added disk support to tron on mesos [\#610](https://github.com/Yelp/Tron/pull/610) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.9.10](https://github.com/Yelp/Tron/tree/v0.9.9.10) (2019-01-31)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.9...v0.9.9.10)

**Merged pull requests:**

- Fix job appearing pending when waiting on trigger requirement [\#611](https://github.com/Yelp/Tron/pull/611) ([keymone](https://github.com/keymone))
- Bump pysensu-yelp to pull in JIRA priority support [\#609](https://github.com/Yelp/Tron/pull/609) ([jvperrin](https://github.com/jvperrin))
- Add ym format [\#608](https://github.com/Yelp/Tron/pull/608) ([keymone](https://github.com/keymone))
- Tron 231 [\#607](https://github.com/Yelp/Tron/pull/607) ([EmanekaT](https://github.com/EmanekaT))

## [v0.9.9.9](https://github.com/Yelp/Tron/tree/v0.9.9.9) (2019-01-07)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.8...v0.9.9.9)

**Merged pull requests:**

- Don't pass in task id when submitting retries of mesos actions [\#605](https://github.com/Yelp/Tron/pull/605) ([qui](https://github.com/qui))
- Use the correct date option for tronctl backfill [\#604](https://github.com/Yelp/Tron/pull/604) ([solarkennedy](https://github.com/solarkennedy))
- Remove less useful stuff [\#584](https://github.com/Yelp/Tron/pull/584) ([keymone](https://github.com/keymone))

## [v0.9.9.8](https://github.com/Yelp/Tron/tree/v0.9.9.8) (2018-12-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.7...v0.9.9.8)

**Merged pull requests:**

- Skip job runs with no action runs during recovery [\#603](https://github.com/Yelp/Tron/pull/603) ([qui](https://github.com/qui))

## [v0.9.9.7](https://github.com/Yelp/Tron/tree/v0.9.9.7) (2018-12-18)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.6...v0.9.9.7)

**Merged pull requests:**

- convert volumes to dict when recovering [\#602](https://github.com/Yelp/Tron/pull/602) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Only recover unknown Mesos actions that have no end time [\#598](https://github.com/Yelp/Tron/pull/598) ([qui](https://github.com/qui))

## [v0.9.9.6](https://github.com/Yelp/Tron/tree/v0.9.9.6) (2018-12-06)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.5...v0.9.9.6)

**Merged pull requests:**

- Fixes for inactive framework [\#601](https://github.com/Yelp/Tron/pull/601) ([qui](https://github.com/qui))

## [v0.9.9.5](https://github.com/Yelp/Tron/tree/v0.9.9.5) (2018-12-04)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.4...v0.9.9.5)

**Merged pull requests:**

- Don't try to use an ssh-agent for the default config [\#600](https://github.com/Yelp/Tron/pull/600) ([solarkennedy](https://github.com/solarkennedy))
- Allow passing extra options to systemd-based distros. [\#599](https://github.com/Yelp/Tron/pull/599) ([solarkennedy](https://github.com/solarkennedy))
- Set an end time if and only if the action should not be recovered [\#597](https://github.com/Yelp/Tron/pull/597) ([qui](https://github.com/qui))
- job should not be rescheduled when reconfiguration if it is disabled [\#596](https://github.com/Yelp/Tron/pull/596) ([chlgit](https://github.com/chlgit))
- Mesos action run should fail if it gets an offer timeout event [\#595](https://github.com/Yelp/Tron/pull/595) ([qui](https://github.com/qui))
- Trigger attribute docs [\#594](https://github.com/Yelp/Tron/pull/594) ([keymone](https://github.com/keymone))
- Add consistency check on Tron startup [\#593](https://github.com/Yelp/Tron/pull/593) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.9.4](https://github.com/Yelp/Tron/tree/v0.9.9.4) (2018-11-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.3...v0.9.9.4)

**Merged pull requests:**

- Fix signal handling in tron daemon [\#592](https://github.com/Yelp/Tron/pull/592) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Log more information from terminal Mesos events [\#590](https://github.com/Yelp/Tron/pull/590) ([qui](https://github.com/qui))

## [v0.9.9.3](https://github.com/Yelp/Tron/tree/v0.9.9.3) (2018-11-15)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.2...v0.9.9.3)

**Merged pull requests:**

- Don't schedule jobs if they are disabled when renaming namespace [\#588](https://github.com/Yelp/Tron/pull/588) ([chlgit](https://github.com/chlgit))
- Removed spaces in action run html [\#587](https://github.com/Yelp/Tron/pull/587) ([solarkennedy](https://github.com/solarkennedy))

## [v0.9.9.2](https://github.com/Yelp/Tron/tree/v0.9.9.2) (2018-11-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.1...v0.9.9.2)

**Merged pull requests:**

- Workaround existing rendered commands that are bytes instead of str [\#586](https://github.com/Yelp/Tron/pull/586) ([keymone](https://github.com/keymone))
- Fix start bug [\#585](https://github.com/Yelp/Tron/pull/585) ([qui](https://github.com/qui))
- Add cluster option to metrics script [\#583](https://github.com/Yelp/Tron/pull/583) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Added --job flag to migrate only a single job between namespaces [\#582](https://github.com/Yelp/Tron/pull/582) ([jordanleex13](https://github.com/jordanleex13))
- Allow TASK\_LOST updates to retry automatically [\#581](https://github.com/Yelp/Tron/pull/581) ([qui](https://github.com/qui))
- Don't display actions/jobs in sorted order in tronview [\#579](https://github.com/Yelp/Tron/pull/579) ([solarkennedy](https://github.com/solarkennedy))
- Trigger timeout attribute [\#578](https://github.com/Yelp/Tron/pull/578) ([keymone](https://github.com/keymone))
- Allow tron to run after unclean shutdown [\#573](https://github.com/Yelp/Tron/pull/573) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.9.1](https://github.com/Yelp/Tron/tree/v0.9.9.1) (2018-10-30)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.9.0...v0.9.9.1)

**Merged pull requests:**

- modify action\_id during job migration [\#580](https://github.com/Yelp/Tron/pull/580) ([chlgit](https://github.com/chlgit))

## [v0.9.9.0](https://github.com/Yelp/Tron/tree/v0.9.9.0) (2018-10-30)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.8.4...v0.9.9.0)

**Merged pull requests:**

- Pin requirements again [\#577](https://github.com/Yelp/Tron/pull/577) ([keymone](https://github.com/keymone))
- Combine DST code into one place [\#572](https://github.com/Yelp/Tron/pull/572) ([qui](https://github.com/qui))
- Add get\_tron\_metrics script and tests [\#571](https://github.com/Yelp/Tron/pull/571) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Tronctl publish/discard events [\#564](https://github.com/Yelp/Tron/pull/564) ([keymone](https://github.com/keymone))

## [v0.9.8.4](https://github.com/Yelp/Tron/tree/v0.9.8.4) (2018-10-26)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.8.3...v0.9.8.4)

## [v0.9.8.3](https://github.com/Yelp/Tron/tree/v0.9.8.3) (2018-10-26)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.8.2...v0.9.8.3)

**Merged pull requests:**

- yapf -rip [\#576](https://github.com/Yelp/Tron/pull/576) ([keymone](https://github.com/keymone))
- Pin and cleanup dependencies [\#570](https://github.com/Yelp/Tron/pull/570) ([keymone](https://github.com/keymone))

## [v0.9.8.2](https://github.com/Yelp/Tron/tree/v0.9.8.2) (2018-10-25)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.8.1...v0.9.8.2)

**Merged pull requests:**

- rename all job\_name after migration [\#574](https://github.com/Yelp/Tron/pull/574) ([chlgit](https://github.com/chlgit))
- add migration namespace script [\#568](https://github.com/Yelp/Tron/pull/568) ([chlgit](https://github.com/chlgit))
- ensure systemd restarts on failure [\#565](https://github.com/Yelp/Tron/pull/565) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.9.8.1](https://github.com/Yelp/Tron/tree/v0.9.8.1) (2018-10-23)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.8.0...v0.9.8.1)

**Merged pull requests:**

- Don't log other signals [\#569](https://github.com/Yelp/Tron/pull/569) ([qui](https://github.com/qui))

## [v0.9.8.0](https://github.com/Yelp/Tron/tree/v0.9.8.0) (2018-10-23)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.7.0...v0.9.8.0)

**Merged pull requests:**

- Fix check\_tron\_jobs to discard instead of remove precioius attr [\#567](https://github.com/Yelp/Tron/pull/567) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- If runs are allowed to overlap, don't consider that case stuck [\#566](https://github.com/Yelp/Tron/pull/566) ([qui](https://github.com/qui))
- Prevent Pymesos thread abort from interrupting main thread [\#560](https://github.com/Yelp/Tron/pull/560) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Initial metrics endpoint [\#559](https://github.com/Yelp/Tron/pull/559) ([qui](https://github.com/qui))
- Add option for alerting every job in check\_tron\_jobs [\#552](https://github.com/Yelp/Tron/pull/552) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.7.0](https://github.com/Yelp/Tron/tree/v0.9.7.0) (2018-10-17)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.5...v0.9.7.0)

**Merged pull requests:**

- Convert action deps to list in repr adapter [\#563](https://github.com/Yelp/Tron/pull/563) ([keymone](https://github.com/keymone))
- \[wip\] systemd unit file [\#557](https://github.com/Yelp/Tron/pull/557) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.9.6.5](https://github.com/Yelp/Tron/tree/v0.9.6.5) (2018-10-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.4...v0.9.6.5)

**Merged pull requests:**

- Fix actiongraph adapter failing to render dependent actions [\#561](https://github.com/Yelp/Tron/pull/561) ([keymone](https://github.com/keymone))

## [v0.9.6.4](https://github.com/Yelp/Tron/tree/v0.9.6.4) (2018-10-15)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.3...v0.9.6.4)

## [v0.9.6.3](https://github.com/Yelp/Tron/tree/v0.9.6.3) (2018-10-15)
[Full Changelog](https://github.com/Yelp/Tron/compare/v...v0.9.6.3)

## [v](https://github.com/Yelp/Tron/tree/v) (2018-10-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.2...v)

**Merged pull requests:**

- Triggers in tronweb [\#558](https://github.com/Yelp/Tron/pull/558) ([keymone](https://github.com/keymone))
- Handle non-Mesos events from taskproc [\#556](https://github.com/Yelp/Tron/pull/556) ([qui](https://github.com/qui))
- add tronctl move command [\#555](https://github.com/Yelp/Tron/pull/555) ([chlgit](https://github.com/chlgit))
- create pidfile for manhole socket [\#554](https://github.com/Yelp/Tron/pull/554) ([chlgit](https://github.com/chlgit))
- update docs using format string [\#553](https://github.com/Yelp/Tron/pull/553) ([chlgit](https://github.com/chlgit))
- Fix constraints passing incorrectly in actionrun [\#551](https://github.com/Yelp/Tron/pull/551) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- External dependencies in tronview [\#550](https://github.com/Yelp/Tron/pull/550) ([keymone](https://github.com/keymone))
- Move bin/\*.py to tron/bin to fix tests [\#549](https://github.com/Yelp/Tron/pull/549) ([keymone](https://github.com/keymone))
- Fetch 20 jobs of history instead of 10 for check\_tron\_jobs [\#547](https://github.com/Yelp/Tron/pull/547) ([solarkennedy](https://github.com/solarkennedy))
- Change tronfig updates to include the enabled attribute [\#545](https://github.com/Yelp/Tron/pull/545) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Remove http stacktraces from tronview and tronctl error messages [\#541](https://github.com/Yelp/Tron/pull/541) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.6.2](https://github.com/Yelp/Tron/tree/v0.9.6.2) (2018-10-02)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.1...v0.9.6.2)

**Merged pull requests:**

- Skip state diagram in docs [\#548](https://github.com/Yelp/Tron/pull/548) ([keymone](https://github.com/keymone))
- Better date arithmetic [\#546](https://github.com/Yelp/Tron/pull/546) ([keymone](https://github.com/keymone))
- Convert Action to dataclass [\#544](https://github.com/Yelp/Tron/pull/544) ([keymone](https://github.com/keymone))
- \[fix\] Eventbus reload [\#543](https://github.com/Yelp/Tron/pull/543) ([keymone](https://github.com/keymone))
- \[cleanup/refactoring\] Move some stuff out of tron.core.job [\#542](https://github.com/Yelp/Tron/pull/542) ([keymone](https://github.com/keymone))
- Remove to\_timestamp method in favor of datetime.timestamp\(\) [\#540](https://github.com/Yelp/Tron/pull/540) ([qui](https://github.com/qui))
- Allow actions to be a dict instead of a list [\#536](https://github.com/Yelp/Tron/pull/536) ([qui](https://github.com/qui))
- Simpler state machine [\#535](https://github.com/Yelp/Tron/pull/535) ([keymone](https://github.com/keymone))
- Actionrun external deps [\#532](https://github.com/Yelp/Tron/pull/532) ([keymone](https://github.com/keymone))
- Make eventbus a global object [\#531](https://github.com/Yelp/Tron/pull/531) ([keymone](https://github.com/keymone))

## [v0.9.6.1](https://github.com/Yelp/Tron/tree/v0.9.6.1) (2018-09-21)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.6.0...v0.9.6.1)

**Merged pull requests:**

- fix format string bugs [\#538](https://github.com/Yelp/Tron/pull/538) ([chlgit](https://github.com/chlgit))
- Add handlers check in MesosTask logger to prevent duplicate log lines [\#537](https://github.com/Yelp/Tron/pull/537) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.6.0](https://github.com/Yelp/Tron/tree/v0.9.6.0) (2018-09-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.5.1...v0.9.6.0)

**Merged pull requests:**

- remove percent string support [\#534](https://github.com/Yelp/Tron/pull/534) ([chlgit](https://github.com/chlgit))
- replace colon by pound for format string [\#530](https://github.com/Yelp/Tron/pull/530) ([chlgit](https://github.com/chlgit))
- Make fail final and move retries and exit logic out [\#529](https://github.com/Yelp/Tron/pull/529) ([qui](https://github.com/qui))
- Cross-job deps: fixes in  config  parsing and eventbus [\#528](https://github.com/Yelp/Tron/pull/528) ([keymone](https://github.com/keymone))
- Cross-job deps: Actionrun triggers upon completion [\#527](https://github.com/Yelp/Tron/pull/527) ([keymone](https://github.com/keymone))
- Removed tronfig header code, as part of removing autogenerated comments [\#521](https://github.com/Yelp/Tron/pull/521) ([kawaiwanyelp](https://github.com/kawaiwanyelp))

## [v0.9.5.1](https://github.com/Yelp/Tron/tree/v0.9.5.1) (2018-09-10)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.5.0...v0.9.5.1)

**Fixed bugs:**

- Gracefully reconfigure service on node change [\#228](https://github.com/Yelp/Tron/issues/228)

**Closed issues:**

- tronview/tronctl should give autocorrect hints [\#98](https://github.com/Yelp/Tron/issues/98)
- Use utcnow\(\) instead of now\(\) [\#83](https://github.com/Yelp/Tron/issues/83)
- Support date range for --run-date [\#64](https://github.com/Yelp/Tron/issues/64)

**Merged pull requests:**

- Fix: restarting tron will restore outdated scheduled jobs [\#525](https://github.com/Yelp/Tron/pull/525) ([keymone](https://github.com/keymone))
- Scheme optional for Mesos master address [\#524](https://github.com/Yelp/Tron/pull/524) ([qui](https://github.com/qui))
- Fix colors on tronweb [\#523](https://github.com/Yelp/Tron/pull/523) ([solarkennedy](https://github.com/solarkennedy))
- Better feedback on killing Mesos actions if not running [\#522](https://github.com/Yelp/Tron/pull/522) ([qui](https://github.com/qui))

## [v0.9.5.0](https://github.com/Yelp/Tron/tree/v0.9.5.0) (2018-09-05)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.4.0...v0.9.5.0)

**Merged pull requests:**

- Increase upstart timeout [\#520](https://github.com/Yelp/Tron/pull/520) ([qui](https://github.com/qui))
- Remove enableall and disableall from JobCollections in tron controller [\#519](https://github.com/Yelp/Tron/pull/519) ([kawaiwanyelp](https://github.com/kawaiwanyelp))
- Deprecate --nodaemon option [\#518](https://github.com/Yelp/Tron/pull/518) ([keymone](https://github.com/keymone))

## [v0.9.4.0](https://github.com/Yelp/Tron/tree/v0.9.4.0) (2018-09-04)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.3.0...v0.9.4.0)

**Merged pull requests:**

- Safer eventbus log saving [\#517](https://github.com/Yelp/Tron/pull/517) ([keymone](https://github.com/keymone))
- Safer shutdown behavior [\#516](https://github.com/Yelp/Tron/pull/516) ([keymone](https://github.com/keymone))
- Eventbus sync shutdown [\#515](https://github.com/Yelp/Tron/pull/515) ([keymone](https://github.com/keymone))
- Yapfify [\#514](https://github.com/Yelp/Tron/pull/514) ([keymone](https://github.com/keymone))
- Recover unknown mesos actions on startup [\#512](https://github.com/Yelp/Tron/pull/512) ([qui](https://github.com/qui))
- Add eventbus and cross-job dep related config attributes [\#511](https://github.com/Yelp/Tron/pull/511) ([keymone](https://github.com/keymone))
- add string format support [\#490](https://github.com/Yelp/Tron/pull/490) ([chlgit](https://github.com/chlgit))

## [v0.9.3.0](https://github.com/Yelp/Tron/tree/v0.9.3.0) (2018-08-24)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.2.1...v0.9.3.0)

**Merged pull requests:**

- make master address optional [\#513](https://github.com/Yelp/Tron/pull/513) ([chlgit](https://github.com/chlgit))
- move mesos address to mesos option section only [\#509](https://github.com/Yelp/Tron/pull/509) ([chlgit](https://github.com/chlgit))
- Added the prototype of the tronctl backfill command [\#507](https://github.com/Yelp/Tron/pull/507) ([solarkennedy](https://github.com/solarkennedy))
- Added mesos framework authentication to itests [\#506](https://github.com/Yelp/Tron/pull/506) ([solarkennedy](https://github.com/solarkennedy))
- Remove event.py and related code [\#504](https://github.com/Yelp/Tron/pull/504) ([keymone](https://github.com/keymone))
- Event bus with pub/sub and persistance [\#497](https://github.com/Yelp/Tron/pull/497) ([keymone](https://github.com/keymone))

## [v0.9.2.1](https://github.com/Yelp/Tron/tree/v0.9.2.1) (2018-08-22)
[Full Changelog](https://github.com/Yelp/Tron/compare/list...v0.9.2.1)

## [list](https://github.com/Yelp/Tron/tree/list) (2018-08-21)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.2.0...list)

**Closed issues:**

- Insufficient error logging [\#275](https://github.com/Yelp/Tron/issues/275)
- tronview add-ons  [\#244](https://github.com/Yelp/Tron/issues/244)
- Replace Turtle with mock [\#153](https://github.com/Yelp/Tron/issues/153)
- tron should support weights for items in pools [\#56](https://github.com/Yelp/Tron/issues/56)

**Merged pull requests:**

- Nix testify [\#508](https://github.com/Yelp/Tron/pull/508) ([solarkennedy](https://github.com/solarkennedy))
- Remove the use of turtle in favor of mock. Fixes \#153 [\#505](https://github.com/Yelp/Tron/pull/505) ([solarkennedy](https://github.com/solarkennedy))
- Run trond via tox for itests instead of pip installing [\#501](https://github.com/Yelp/Tron/pull/501) ([solarkennedy](https://github.com/solarkennedy))
- Made tronview and tronctl provide suggestions on unknown identifiers [\#500](https://github.com/Yelp/Tron/pull/500) ([solarkennedy](https://github.com/solarkennedy))
- Make itests wait for tron to be connected to mesos [\#499](https://github.com/Yelp/Tron/pull/499) ([solarkennedy](https://github.com/solarkennedy))
- U/chl/save mesos state [\#498](https://github.com/Yelp/Tron/pull/498) ([chlgit](https://github.com/chlgit))
- save task id of action run [\#495](https://github.com/Yelp/Tron/pull/495) ([chlgit](https://github.com/chlgit))
- Added Tron + Mesos itest framework [\#494](https://github.com/Yelp/Tron/pull/494) ([solarkennedy](https://github.com/solarkennedy))
- configure mesos authentication at MASTER [\#492](https://github.com/Yelp/Tron/pull/492) ([chlgit](https://github.com/chlgit))

## [v0.9.2.0](https://github.com/Yelp/Tron/tree/v0.9.2.0) (2018-08-09)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.9...v0.9.2.0)

**Merged pull requests:**

- Autospec all the things [\#491](https://github.com/Yelp/Tron/pull/491) ([solarkennedy](https://github.com/solarkennedy))
- Spell check all the things [\#489](https://github.com/Yelp/Tron/pull/489) ([solarkennedy](https://github.com/solarkennedy))
- catch command rendering type error [\#488](https://github.com/Yelp/Tron/pull/488) ([chlgit](https://github.com/chlgit))
- Update taskproc [\#487](https://github.com/Yelp/Tron/pull/487) ([qui](https://github.com/qui))
- tronfig checks valid nodes [\#486](https://github.com/Yelp/Tron/pull/486) ([chlgit](https://github.com/chlgit))
- Catch errors from commands and return a nicer error message [\#485](https://github.com/Yelp/Tron/pull/485) ([qui](https://github.com/qui))
- sort the job run based on endtime and scheduled time [\#484](https://github.com/Yelp/Tron/pull/484) ([chlgit](https://github.com/chlgit))

## [v0.9.1.9](https://github.com/Yelp/Tron/tree/v0.9.1.9) (2018-07-24)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.8...v0.9.1.9)

**Fixed bugs:**

- Refactor tron node/ssh [\#247](https://github.com/Yelp/Tron/issues/247)

**Closed issues:**

- Tron uses an ancient ssh key exchange algorithm [\#323](https://github.com/Yelp/Tron/issues/323)
- Problem start trond [\#307](https://github.com/Yelp/Tron/issues/307)

**Merged pull requests:**

- Retries delay: kill delayed action correctly [\#483](https://github.com/Yelp/Tron/pull/483) ([keymone](https://github.com/keymone))
- send alerts based on job runtime [\#482](https://github.com/Yelp/Tron/pull/482) ([chlgit](https://github.com/chlgit))
- Fix the docs [\#481](https://github.com/Yelp/Tron/pull/481) ([solarkennedy](https://github.com/solarkennedy))
- Delay between actionrun retries [\#479](https://github.com/Yelp/Tron/pull/479) ([keymone](https://github.com/keymone))

## [v0.9.1.8](https://github.com/Yelp/Tron/tree/v0.9.1.8) (2018-07-10)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.7...v0.9.1.8)

**Merged pull requests:**

- fix \_get\_seconds\_from\_duration bug [\#480](https://github.com/Yelp/Tron/pull/480) ([chlgit](https://github.com/chlgit))

## [v0.9.1.7](https://github.com/Yelp/Tron/tree/v0.9.1.7) (2018-07-09)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.6...v0.9.1.7)

**Merged pull requests:**

- Fix validation of full tronfig directory [\#478](https://github.com/Yelp/Tron/pull/478) ([qui](https://github.com/qui))
- Unknown actions/jobs are critical alerts [\#477](https://github.com/Yelp/Tron/pull/477) ([qui](https://github.com/qui))
- Twisted manhole on unix socket [\#476](https://github.com/Yelp/Tron/pull/476) ([keymone](https://github.com/keymone))

## [v0.9.1.6](https://github.com/Yelp/Tron/tree/v0.9.1.6) (2018-07-03)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.4...v0.9.1.6)

**Merged pull requests:**

- wait daemon thread finish before exit [\#475](https://github.com/Yelp/Tron/pull/475) ([chlgit](https://github.com/chlgit))
- Remove the graceful\_shutdown aspect of Tron [\#474](https://github.com/Yelp/Tron/pull/474) ([solarkennedy](https://github.com/solarkennedy))
- fix tronfig -C bug [\#473](https://github.com/Yelp/Tron/pull/473) ([chlgit](https://github.com/chlgit))
- add test case for long duration jobs [\#472](https://github.com/Yelp/Tron/pull/472) ([chlgit](https://github.com/chlgit))
- fix duration is longer than 1 day bug [\#471](https://github.com/Yelp/Tron/pull/471) ([chlgit](https://github.com/chlgit))
- Check output dir first [\#470](https://github.com/Yelp/Tron/pull/470) ([qui](https://github.com/qui))
- Add default volumes and some other Mesos task settings to master config [\#466](https://github.com/Yelp/Tron/pull/466) ([qui](https://github.com/qui))
- implement kill terminate for mesos actions [\#464](https://github.com/Yelp/Tron/pull/464) ([chlgit](https://github.com/chlgit))

## [v0.9.1.4](https://github.com/Yelp/Tron/tree/v0.9.1.4) (2018-06-25)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.3...v0.9.1.4)

**Merged pull requests:**

- action\_runner logs to the output\_dir; add timestamps to logs [\#469](https://github.com/Yelp/Tron/pull/469) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.9.1.3](https://github.com/Yelp/Tron/tree/v0.9.1.3) (2018-06-22)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.2...v0.9.1.3)

**Merged pull requests:**

- handle failures streaming  to stdout/stderr [\#468](https://github.com/Yelp/Tron/pull/468) ([Rob-Johnson](https://github.com/Rob-Johnson))
- reset exit status on recovery [\#465](https://github.com/Yelp/Tron/pull/465) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.9.1.2](https://github.com/Yelp/Tron/tree/v0.9.1.2) (2018-06-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.1...v0.9.1.2)

**Merged pull requests:**

- add missing requests and psutil deps [\#462](https://github.com/Yelp/Tron/pull/462) ([Rob-Johnson](https://github.com/Rob-Johnson))
- reset actionrun endtime on recovery [\#461](https://github.com/Yelp/Tron/pull/461) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Refactor config parse tests [\#460](https://github.com/Yelp/Tron/pull/460) ([keymone](https://github.com/keymone))
- Improve exit handling [\#459](https://github.com/Yelp/Tron/pull/459) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Toggle for Mesos actions [\#458](https://github.com/Yelp/Tron/pull/458) ([qui](https://github.com/qui))
- Add flake8 hook, make it stop arguing with yapf [\#457](https://github.com/Yelp/Tron/pull/457) ([keymone](https://github.com/keymone))
- deploy prod namespaces and jobs at playground [\#449](https://github.com/Yelp/Tron/pull/449) ([chlgit](https://github.com/chlgit))

## [v0.9.1.1](https://github.com/Yelp/Tron/tree/v0.9.1.1) (2018-06-15)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.1.0...v0.9.1.1)

**Merged pull requests:**

- stream action\_runner output from subprocess [\#456](https://github.com/Yelp/Tron/pull/456) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Bounded asynchronous API request handling [\#455](https://github.com/Yelp/Tron/pull/455) ([keymone](https://github.com/keymone))
- ClusterRepository for accessing Mesos clusters [\#454](https://github.com/Yelp/Tron/pull/454) ([qui](https://github.com/qui))

## [v0.9.1.0](https://github.com/Yelp/Tron/tree/v0.9.1.0) (2018-06-13)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.9.0.0...v0.9.1.0)

**Merged pull requests:**

- exit the recovery batch with the correct exit code [\#453](https://github.com/Yelp/Tron/pull/453) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Get output from Mesos tasks [\#452](https://github.com/Yelp/Tron/pull/452) ([qui](https://github.com/qui))
- run tasks on Mesos [\#448](https://github.com/Yelp/Tron/pull/448) ([qui](https://github.com/qui))

## [v0.9.0.0](https://github.com/Yelp/Tron/tree/v0.9.0.0) (2018-06-05)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.6...v0.9.0.0)

**Merged pull requests:**

- set the machine state to 'running' before recovery [\#451](https://github.com/Yelp/Tron/pull/451) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Action runner factory should always be an instance [\#450](https://github.com/Yelp/Tron/pull/450) ([qui](https://github.com/qui))
- Revert "Fix reloading config ignores changes in `enabled`" [\#447](https://github.com/Yelp/Tron/pull/447) ([vkhromov](https://github.com/vkhromov))
- Fix reloading config ignores changes in `enabled` [\#446](https://github.com/Yelp/Tron/pull/446) ([vkhromov](https://github.com/vkhromov))
- Fixed the example Docker container name in README.md [\#445](https://github.com/Yelp/Tron/pull/445) ([vkhromov](https://github.com/vkhromov))
- Recover unknown batches [\#425](https://github.com/Yelp/Tron/pull/425) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.8.0.6](https://github.com/Yelp/Tron/tree/v0.8.0.6) (2018-05-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.5...v0.8.0.6)

**Fixed bugs:**

- node reconfig should not cause all services to restart [\#223](https://github.com/Yelp/Tron/issues/223)

**Closed issues:**

- Enhancement: "tron restart", but for actions rather than jobs. [\#309](https://github.com/Yelp/Tron/issues/309)
- Support retrying action runs [\#120](https://github.com/Yelp/Tron/issues/120)

**Merged pull requests:**

- add expected runtime for cleanup actions [\#444](https://github.com/Yelp/Tron/pull/444) ([chlgit](https://github.com/chlgit))
- bug fixes: namespace deletion, reading long status files [\#443](https://github.com/Yelp/Tron/pull/443) ([qui](https://github.com/qui))
- fix a bug at check\_tron\_jobs scripts [\#442](https://github.com/Yelp/Tron/pull/442) ([chlgit](https://github.com/chlgit))
- set check\_every in monitoring script [\#441](https://github.com/Yelp/Tron/pull/441) ([chlgit](https://github.com/chlgit))
- Added docker-compose dependency to fix example-cluster on Trusty [\#440](https://github.com/Yelp/Tron/pull/440) ([vkhromov](https://github.com/vkhromov))
- Chl/use indent dictionary value for yapf [\#439](https://github.com/Yelp/Tron/pull/439) ([chlgit](https://github.com/chlgit))
- add support for expected\_runtime alerting [\#438](https://github.com/Yelp/Tron/pull/438) ([chlgit](https://github.com/chlgit))
- Pre calculate state machine transitions [\#437](https://github.com/Yelp/Tron/pull/437) ([keymone](https://github.com/keymone))
- Added retry docs [\#436](https://github.com/Yelp/Tron/pull/436) ([solarkennedy](https://github.com/solarkennedy))
- Yapf formatting [\#433](https://github.com/Yelp/Tron/pull/433) ([keymone](https://github.com/keymone))
- Remove tron/utils/collections [\#387](https://github.com/Yelp/Tron/pull/387) ([keymone](https://github.com/keymone))

## [v0.8.0.5](https://github.com/Yelp/Tron/tree/v0.8.0.5) (2018-04-24)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.4...v0.8.0.5)

**Merged pull requests:**

- maybe\_encode all data in the file serializer [\#435](https://github.com/Yelp/Tron/pull/435) ([qui](https://github.com/qui))

## [v0.8.0.4](https://github.com/Yelp/Tron/tree/v0.8.0.4) (2018-04-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.3...v0.8.0.4)

## [v0.8.0.3](https://github.com/Yelp/Tron/tree/v0.8.0.3) (2018-04-20)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.2...v0.8.0.3)

**Merged pull requests:**

- Fix Xenial build [\#432](https://github.com/Yelp/Tron/pull/432) ([vkhromov](https://github.com/vkhromov))
- Add retry action [\#418](https://github.com/Yelp/Tron/pull/418) ([keymone](https://github.com/keymone))

## [v0.8.0.2](https://github.com/Yelp/Tron/tree/v0.8.0.2) (2018-04-18)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.1...v0.8.0.2)

**Merged pull requests:**

- Fix scheduled jobs page [\#431](https://github.com/Yelp/Tron/pull/431) ([keymone](https://github.com/keymone))
- Some cleanups and fix to config manager [\#430](https://github.com/Yelp/Tron/pull/430) ([keymone](https://github.com/keymone))

## [v0.8.0.1](https://github.com/Yelp/Tron/tree/v0.8.0.1) (2018-04-16)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.8.3...v0.8.0.1)

**Merged pull requests:**

- Always send signals to the full process group in the tron action runner [\#428](https://github.com/Yelp/Tron/pull/428) ([solarkennedy](https://github.com/solarkennedy))
- speed up tronview command with zero args [\#427](https://github.com/Yelp/Tron/pull/427) ([chlgit](https://github.com/chlgit))
- Python3 deb [\#370](https://github.com/Yelp/Tron/pull/370) ([keymone](https://github.com/keymone))

## [v0.7.8.3](https://github.com/Yelp/Tron/tree/v0.7.8.3) (2018-04-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.8.2...v0.7.8.3)

**Closed issues:**

- Node healthchecks [\#216](https://github.com/Yelp/Tron/issues/216)
- Tab complete tronview commands [\#35](https://github.com/Yelp/Tron/issues/35)

**Merged pull requests:**

- Maybe decode all the things [\#426](https://github.com/Yelp/Tron/pull/426) ([keymone](https://github.com/keymone))
- Fix links in long\_description \(used on PyPI\), use hyperlinks [\#423](https://github.com/Yelp/Tron/pull/423) ([sjaensch](https://github.com/sjaensch))
- Run the example\_cluster under faketime 10x faster than real life [\#415](https://github.com/Yelp/Tron/pull/415) ([solarkennedy](https://github.com/solarkennedy))

## [v0.7.8.2](https://github.com/Yelp/Tron/tree/v0.7.8.2) (2018-04-10)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.8.1...v0.7.8.2)

**Merged pull requests:**

- Fix validation of cleanup action [\#424](https://github.com/Yelp/Tron/pull/424) ([keymone](https://github.com/keymone))
- set a smart realert\_every [\#420](https://github.com/Yelp/Tron/pull/420) ([chlgit](https://github.com/chlgit))

## [v0.7.8.1](https://github.com/Yelp/Tron/tree/v0.7.8.1) (2018-04-06)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.8.0...v0.7.8.1)

**Merged pull requests:**

- Deal with requestargs encoding in get\_string\(\) [\#422](https://github.com/Yelp/Tron/pull/422) ([keymone](https://github.com/keymone))
- monitoring alert if any action becomes failed or unknown [\#421](https://github.com/Yelp/Tron/pull/421) ([chlgit](https://github.com/chlgit))
- Fix namespace with non-ascii crashing tronfig [\#419](https://github.com/Yelp/Tron/pull/419) ([keymone](https://github.com/keymone))
- Disconnect state version from release version [\#417](https://github.com/Yelp/Tron/pull/417) ([keymone](https://github.com/keymone))
- service code clean up [\#416](https://github.com/Yelp/Tron/pull/416) ([chlgit](https://github.com/chlgit))
- Alert when a job is in an unknown state [\#413](https://github.com/Yelp/Tron/pull/413) ([solarkennedy](https://github.com/solarkennedy))
- Added the action\_runner for localhost on the example\_cluster [\#407](https://github.com/Yelp/Tron/pull/407) ([solarkennedy](https://github.com/solarkennedy))
- remove service code third pass [\#405](https://github.com/Yelp/Tron/pull/405) ([chlgit](https://github.com/chlgit))
- First Stab at Action retries [\#401](https://github.com/Yelp/Tron/pull/401) ([keymone](https://github.com/keymone))

## [v0.7.8.0](https://github.com/Yelp/Tron/tree/v0.7.8.0) (2018-04-03)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.7.1...v0.7.8.0)

**Merged pull requests:**

- Don't delete file if namespace is incorrect [\#414](https://github.com/Yelp/Tron/pull/414) ([qui](https://github.com/qui))
- update namespace cleanup script [\#411](https://github.com/Yelp/Tron/pull/411) ([chlgit](https://github.com/chlgit))
- Fix timestamp with timezone and remove flaky test [\#410](https://github.com/Yelp/Tron/pull/410) ([qui](https://github.com/qui))
- add a script to clean up namespace [\#409](https://github.com/Yelp/Tron/pull/409) ([chlgit](https://github.com/chlgit))
- improve monitoring log [\#408](https://github.com/Yelp/Tron/pull/408) ([chlgit](https://github.com/chlgit))
- Try to fix doc building [\#406](https://github.com/Yelp/Tron/pull/406) ([solarkennedy](https://github.com/solarkennedy))
- fix the action\_status command [\#404](https://github.com/Yelp/Tron/pull/404) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Timezone date context bug [\#403](https://github.com/Yelp/Tron/pull/403) ([qui](https://github.com/qui))
- Do a better job at printing exceptions when there are command render problems [\#402](https://github.com/Yelp/Tron/pull/402) ([solarkennedy](https://github.com/solarkennedy))
- Make more sane tab completions for busy tron servers [\#400](https://github.com/Yelp/Tron/pull/400) ([solarkennedy](https://github.com/solarkennedy))
- Chl/remove service code second pass [\#399](https://github.com/Yelp/Tron/pull/399) ([chlgit](https://github.com/chlgit))
- Use PaaSTA config values in runs and create PaaSTA action run [\#398](https://github.com/Yelp/Tron/pull/398) ([qui](https://github.com/qui))

## [v0.7.7.1](https://github.com/Yelp/Tron/tree/v0.7.7.1) (2018-03-23)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.7.0...v0.7.7.1)

**Closed issues:**

- Document example of setting a non-DST-aware time zone [\#82](https://github.com/Yelp/Tron/issues/82)

**Merged pull requests:**

- Yelp's itest changes [\#397](https://github.com/Yelp/Tron/pull/397) ([keymone](https://github.com/keymone))
- add header required for CORS [\#396](https://github.com/Yelp/Tron/pull/396) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Use bsddb3 directly [\#395](https://github.com/Yelp/Tron/pull/395) ([keymone](https://github.com/keymone))
- Removed service functionality from tronweb [\#394](https://github.com/Yelp/Tron/pull/394) ([solarkennedy](https://github.com/solarkennedy))
- Print out job runs and action runs in tab completion cache [\#393](https://github.com/Yelp/Tron/pull/393) ([solarkennedy](https://github.com/solarkennedy))

## [v0.7.7.0](https://github.com/Yelp/Tron/tree/v0.7.7.0) (2018-03-22)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.8.0.0...v0.7.7.0)

**Merged pull requests:**

- Only try advancing the time by one hour when localizing [\#392](https://github.com/Yelp/Tron/pull/392) ([keymone](https://github.com/keymone))
- use the resolved namespace in tronfig [\#391](https://github.com/Yelp/Tron/pull/391) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Paasta executor configs [\#390](https://github.com/Yelp/Tron/pull/390) ([qui](https://github.com/qui))
- Implement and use backward compatible Py2Shelf [\#389](https://github.com/Yelp/Tron/pull/389) ([keymone](https://github.com/keymone))
- Dont try to localize time with tzinfo [\#388](https://github.com/Yelp/Tron/pull/388) ([keymone](https://github.com/keymone))
- Fix example cluster and package building [\#386](https://github.com/Yelp/Tron/pull/386) ([keymone](https://github.com/keymone))
- Remove service code - first pass [\#385](https://github.com/Yelp/Tron/pull/385) ([chlgit](https://github.com/chlgit))
- fix check\_tron\_job broken due to job misconfigure issue [\#384](https://github.com/Yelp/Tron/pull/384) ([chlgit](https://github.com/chlgit))
- Change location of tab complete cache [\#383](https://github.com/Yelp/Tron/pull/383) ([jglukasik](https://github.com/jglukasik))
- Allow tab complete to read from cached file [\#382](https://github.com/Yelp/Tron/pull/382) ([jglukasik](https://github.com/jglukasik))

## [v0.8.0.0](https://github.com/Yelp/Tron/tree/v0.8.0.0) (2018-03-14)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.6.1...v0.8.0.0)

**Merged pull requests:**

- fix get relevant action bug and add unit tests [\#381](https://github.com/Yelp/Tron/pull/381) ([chlgit](https://github.com/chlgit))

## [v0.7.6.1](https://github.com/Yelp/Tron/tree/v0.7.6.1) (2018-03-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.6.0...v0.7.6.1)

## [v0.7.6.0](https://github.com/Yelp/Tron/tree/v0.7.6.0) (2018-03-12)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.5.3...v0.7.6.0)

**Merged pull requests:**

- First pass at tab complete [\#380](https://github.com/Yelp/Tron/pull/380) ([jglukasik](https://github.com/jglukasik))
- Added a 'make dev' target to iterate faster with local development [\#378](https://github.com/Yelp/Tron/pull/378) ([solarkennedy](https://github.com/solarkennedy))
- Fix CommandIndex constructor to fix autocomplete in tronweb [\#376](https://github.com/Yelp/Tron/pull/376) ([solarkennedy](https://github.com/solarkennedy))
- Python3 port [\#362](https://github.com/Yelp/Tron/pull/362) ([keymone](https://github.com/keymone))

## [v0.7.5.3](https://github.com/Yelp/Tron/tree/v0.7.5.3) (2018-03-08)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.5.2...v0.7.5.3)

**Closed issues:**

- tronweb hijacks cmd+r \(browser refresh\) [\#278](https://github.com/Yelp/Tron/issues/278)

**Merged pull requests:**

- Move tron documentation to readthedocs.io [\#377](https://github.com/Yelp/Tron/pull/377) ([solarkennedy](https://github.com/solarkennedy))
- Remove keybind hijacking. Fixes \#278 [\#375](https://github.com/Yelp/Tron/pull/375) ([solarkennedy](https://github.com/solarkennedy))
- Chl/tron 224 detect jobs not scheduled [\#374](https://github.com/Yelp/Tron/pull/374) ([chlgit](https://github.com/chlgit))
- Upgrade to argparse [\#372](https://github.com/Yelp/Tron/pull/372) ([jglukasik](https://github.com/jglukasik))

## [v0.7.5.2](https://github.com/Yelp/Tron/tree/v0.7.5.2) (2018-03-02)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.5.1...v0.7.5.2)

**Merged pull requests:**

- Only localize datetimes when they lack tzinfo [\#373](https://github.com/Yelp/Tron/pull/373) ([solarkennedy](https://github.com/solarkennedy))
- improving stuck job checking when runtime of job run unsorted [\#365](https://github.com/Yelp/Tron/pull/365) ([chlgit](https://github.com/chlgit))

## [v0.7.5.1](https://github.com/Yelp/Tron/tree/v0.7.5.1) (2018-02-28)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.5.0...v0.7.5.1)

**Merged pull requests:**

- remove owner from tronview display [\#371](https://github.com/Yelp/Tron/pull/371) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Make tron pidfile error message more clear [\#367](https://github.com/Yelp/Tron/pull/367) ([jglukasik](https://github.com/jglukasik))

## [v0.7.5.0](https://github.com/Yelp/Tron/tree/v0.7.5.0) (2018-02-28)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.4.2...v0.7.5.0)

**Merged pull requests:**

- rmove notes/summary/owner [\#368](https://github.com/Yelp/Tron/pull/368) ([chlgit](https://github.com/chlgit))
- Allow jobs to override the default timezone [\#360](https://github.com/Yelp/Tron/pull/360) ([solarkennedy](https://github.com/solarkennedy))

## [v0.7.4.2](https://github.com/Yelp/Tron/tree/v0.7.4.2) (2018-02-27)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.4.1...v0.7.4.2)

**Merged pull requests:**

- Fix tronview bug and include six in install\_requires [\#366](https://github.com/Yelp/Tron/pull/366) ([qui](https://github.com/qui))

## [v0.7.4.1](https://github.com/Yelp/Tron/tree/v0.7.4.1) (2018-02-23)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.4.0...v0.7.4.1)

**Closed issues:**

- Spanish translation [\#361](https://github.com/Yelp/Tron/issues/361)

**Merged pull requests:**

- Failure improvements [\#364](https://github.com/Yelp/Tron/pull/364) ([qui](https://github.com/qui))
- store the actionrun id in the status output by action\_runner [\#363](https://github.com/Yelp/Tron/pull/363) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Ignore unknown ActionRuns [\#359](https://github.com/Yelp/Tron/pull/359) ([qui](https://github.com/qui))
- Upload to pypi on tags [\#357](https://github.com/Yelp/Tron/pull/357) ([solarkennedy](https://github.com/solarkennedy))
- Remove vagrant stuff [\#356](https://github.com/Yelp/Tron/pull/356) ([solarkennedy](https://github.com/solarkennedy))
- add stuck status for jobs [\#355](https://github.com/Yelp/Tron/pull/355) ([chlgit](https://github.com/chlgit))
- reconfigure jobs if monitoring setting has been updated [\#354](https://github.com/Yelp/Tron/pull/354) ([chlgit](https://github.com/chlgit))
- Python3 port 1/NaN [\#353](https://github.com/Yelp/Tron/pull/353) ([keymone](https://github.com/keymone))

## [v0.7.4.0](https://github.com/Yelp/Tron/tree/v0.7.4.0) (2018-02-13)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.3.2...v0.7.4.0)

**Closed issues:**

- Job Notifications [\#303](https://github.com/Yelp/Tron/issues/303)
- smtp with tls [\#257](https://github.com/Yelp/Tron/issues/257)
- nodes list [\#245](https://github.com/Yelp/Tron/issues/245)
- tronweb service view dashboard [\#243](https://github.com/Yelp/Tron/issues/243)
- support ec2 tag names for node\_pool [\#233](https://github.com/Yelp/Tron/issues/233)
- Consolidate documentation [\#135](https://github.com/Yelp/Tron/issues/135)
- Add 'owner' field to jobs [\#99](https://github.com/Yelp/Tron/issues/99)
- Tron daemon to run on slaves as an alternative to persistent SSH connections [\#81](https://github.com/Yelp/Tron/issues/81)
- Monitoring Framework [\#25](https://github.com/Yelp/Tron/issues/25)

**Merged pull requests:**

- \[wip\] Port to Python3 [\#352](https://github.com/Yelp/Tron/pull/352) ([keymone](https://github.com/keymone))
- Removed deprecated restart\_interval option [\#351](https://github.com/Yelp/Tron/pull/351) ([solarkennedy](https://github.com/solarkennedy))
- Removed mongodb support [\#350](https://github.com/Yelp/Tron/pull/350) ([solarkennedy](https://github.com/solarkennedy))
- Update readme to set expectations about tron development [\#349](https://github.com/Yelp/Tron/pull/349) ([solarkennedy](https://github.com/solarkennedy))
- Improve example cluster startup, and fix one remaining unicode error [\#348](https://github.com/Yelp/Tron/pull/348) ([qui](https://github.com/qui))

## [v0.7.3.2](https://github.com/Yelp/Tron/tree/v0.7.3.2) (2018-02-09)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.3.1...v0.7.3.2)

**Merged pull requests:**

- Twisted fix, example cluster and itest improvements [\#347](https://github.com/Yelp/Tron/pull/347) ([keymone](https://github.com/keymone))

## [v0.7.3.1](https://github.com/Yelp/Tron/tree/v0.7.3.1) (2018-02-09)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.3.0...v0.7.3.1)

## [v0.7.3.0](https://github.com/Yelp/Tron/tree/v0.7.3.0) (2018-02-08)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.2.0...v0.7.3.0)

**Merged pull requests:**

- Don't start tron on boot [\#345](https://github.com/Yelp/Tron/pull/345) ([keymone](https://github.com/keymone))
- Added dry-run and filtering on check\_tron\_jobs [\#344](https://github.com/Yelp/Tron/pull/344) ([solarkennedy](https://github.com/solarkennedy))
- Example cluster improvements [\#343](https://github.com/Yelp/Tron/pull/343) ([keymone](https://github.com/keymone))
- Make check\_tron\_jobs actually send alerts with lots of context [\#342](https://github.com/Yelp/Tron/pull/342) ([solarkennedy](https://github.com/solarkennedy))
- Local validation options, cleanup of unused functionality [\#340](https://github.com/Yelp/Tron/pull/340) ([keymone](https://github.com/keymone))
- first pass at adding pre-commit [\#321](https://github.com/Yelp/Tron/pull/321) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.7.2.0](https://github.com/Yelp/Tron/tree/v0.7.2.0) (2018-02-01)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.1.0...v0.7.2.0)

**Closed issues:**

- tron on OS X without Poll Reactor [\#305](https://github.com/Yelp/Tron/issues/305)
- allow viewing by host in tronview [\#138](https://github.com/Yelp/Tron/issues/138)

**Merged pull requests:**

- add coffeescript dependency to packaging image [\#341](https://github.com/Yelp/Tron/pull/341) ([Rob-Johnson](https://github.com/Rob-Johnson))
- run coffeescript compliation inside docker container; remove some unu… [\#339](https://github.com/Yelp/Tron/pull/339) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Add option to delete namespaces in tronfig [\#338](https://github.com/Yelp/Tron/pull/338) ([qui](https://github.com/qui))
- Added monitoring dictionary for job configs [\#337](https://github.com/Yelp/Tron/pull/337) ([solarkennedy](https://github.com/solarkennedy))
- Added rerun for re-running jobs instead of restart [\#336](https://github.com/Yelp/Tron/pull/336) ([solarkennedy](https://github.com/solarkennedy))
- Added prototype check\_tron\_jobs command [\#335](https://github.com/Yelp/Tron/pull/335) ([solarkennedy](https://github.com/solarkennedy))
- add a json schema for describing tronfig [\#334](https://github.com/Yelp/Tron/pull/334) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Handle configuration check requests [\#333](https://github.com/Yelp/Tron/pull/333) ([keymone](https://github.com/keymone))
- Bring back coffee [\#332](https://github.com/Yelp/Tron/pull/332) ([Rob-Johnson](https://github.com/Rob-Johnson))
- move from sysv-init to upstart [\#331](https://github.com/Yelp/Tron/pull/331) ([Rob-Johnson](https://github.com/Rob-Johnson))
- multi platform reactor; fix tests [\#330](https://github.com/Yelp/Tron/pull/330) ([Rob-Johnson](https://github.com/Rob-Johnson))
- encode command as utf-8 [\#329](https://github.com/Yelp/Tron/pull/329) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.7.1.0](https://github.com/Yelp/Tron/tree/v0.7.1.0) (2017-10-10)
[Full Changelog](https://github.com/Yelp/Tron/compare/v0.7.0.0...v0.7.1.0)

**Closed issues:**

- Unsafe use of `global`  in tron/serialize/runstate/yamlstore.py [\#326](https://github.com/Yelp/Tron/issues/326)
- Tron assumes USER environment variable is always available, and it isn't. [\#315](https://github.com/Yelp/Tron/issues/315)
- Use the yaml c-loader for moar speed \(when available\) [\#306](https://github.com/Yelp/Tron/issues/306)

**Merged pull requests:**

- dont assume the USER env var [\#328](https://github.com/Yelp/Tron/pull/328) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Issues 306 & 326  [\#327](https://github.com/Yelp/Tron/pull/327) ([Rob-Johnson](https://github.com/Rob-Johnson))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*