import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useJobs } from "@/lib/queries";
import { StatusCard } from "@/components/StatusCard";
import { StateBadge } from "@/components/StateBadge";
import { formatRelativeTime } from "@/lib/utils";
import type { Job, ActionState } from "@/lib/types";

function getLatestRunState(job: Job): ActionState {
  if (job.runs && job.runs.length > 0) {
    return job.runs[0].state;
  }
  return "unknown";
}

function getLastRunTime(job: Job): string | null {
  if (job.runs && job.runs.length > 0) {
    return job.runs[0].end_time ?? job.runs[0].start_time;
  }
  return null;
}

export function Dashboard() {
  const { data: jobs, isLoading } = useJobs();
  const navigate = useNavigate();

  const stats = useMemo(() => {
    if (!jobs) return { succeeded: 0, failed: 0, running: 0, queued: 0, scheduled: 0 };
    const counts = { succeeded: 0, failed: 0, running: 0, queued: 0, scheduled: 0 };
    for (const job of jobs) {
      const state = getLatestRunState(job);
      if (state === "succeeded") counts.succeeded++;
      else if (state === "failed") counts.failed++;
      else if (state === "running" || state === "starting") counts.running++;
      else if (state === "queued" || state === "waiting") counts.queued++;
      else if (state === "scheduled") counts.scheduled++;
    }
    return counts;
  }, [jobs]);

  const failedJobs = useMemo(() => {
    if (!jobs) return [];
    return jobs
      .filter((j) => getLatestRunState(j) === "failed")
      .sort((a, b) => {
        const aTime = getLastRunTime(a) ?? "";
        const bTime = getLastRunTime(b) ?? "";
        return bTime.localeCompare(aTime);
      })
      .slice(0, 10);
  }, [jobs]);

  const recentActivity = useMemo(() => {
    if (!jobs) return [];
    return jobs
      .filter((j) => j.runs && j.runs.length > 0)
      .sort((a, b) => {
        const aTime = getLastRunTime(a) ?? "";
        const bTime = getLastRunTime(b) ?? "";
        return bTime.localeCompare(aTime);
      })
      .slice(0, 10);
  }, [jobs]);

  if (isLoading) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  return (
    <div className="space-y-6">
      <div className="flex gap-3">
        <StatusCard label="Succeeded" count={stats.succeeded} variant="success" />
        <StatusCard label="Failed" count={stats.failed} variant="danger" />
        <StatusCard label="Running" count={stats.running} variant="info" />
        <StatusCard label="Queued" count={stats.queued} variant="warning" />
        <StatusCard label="Scheduled" count={stats.scheduled} variant="purple" />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg border border-red-200 dark:border-red-900 overflow-hidden">
          <div className="border-b border-red-200 dark:border-red-900 bg-red-50 dark:bg-red-950/30 px-4 py-2.5 text-sm font-semibold text-red-800 dark:text-red-400">
            Failed Jobs ({stats.failed})
          </div>
          <div className="divide-y">
            {failedJobs.map((job) => (
              <div
                key={job.name}
                className="flex items-center justify-between px-4 py-2 text-sm cursor-pointer hover:bg-muted/50"
                onClick={() => navigate(`/job/${job.name}`)}
              >
                <span className="text-primary font-medium">{job.name}</span>
                <span className="text-muted-foreground text-xs">
                  {formatRelativeTime(getLastRunTime(job))}
                </span>
              </div>
            ))}
            {failedJobs.length === 0 && (
              <div className="px-4 py-3 text-sm text-muted-foreground">
                No failed jobs
              </div>
            )}
          </div>
        </div>

        <div className="rounded-lg border overflow-hidden">
          <div className="border-b bg-muted/50 px-4 py-2.5 text-sm font-semibold">
            Recent Activity
          </div>
          <div className="divide-y">
            {recentActivity.map((job) => (
              <div
                key={job.name}
                className="flex items-center gap-2 px-4 py-2 text-sm cursor-pointer hover:bg-muted/50"
                onClick={() => navigate(`/job/${job.name}`)}
              >
                <span className="font-medium">{job.name}</span>
                <StateBadge state={getLatestRunState(job)} />
                <span className="ml-auto text-xs text-muted-foreground">
                  {formatRelativeTime(getLastRunTime(job))}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
