import { useParams, useNavigate, Link } from "react-router-dom";
import { useJob } from "@/lib/queries";
import { StateBadge } from "@/components/StateBadge";
import { ActionGraph } from "@/components/ActionGraph";
import { DataTable, type Column } from "@/components/DataTable";
import { formatRelativeTime, formatDuration } from "@/lib/utils";
import type { JobRun } from "@/lib/types";

export function JobDetail() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const { data: job, isLoading } = useJob(name!);

  if (isLoading || !job) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  const runColumns: Column<JobRun>[] = [
    {
      key: "run_num",
      header: "Run",
      render: (val) => (
        <span className="font-medium text-primary">#{String(val)}</span>
      ),
    },
    {
      key: "state",
      header: "State",
      render: (val) => <StateBadge state={val as JobRun["state"]} />,
    },
    {
      key: "start_time",
      header: "Started",
      render: (val) => (
        <span className="text-muted-foreground">
          {formatRelativeTime(val as string | null)}
        </span>
      ),
    },
    {
      key: "duration",
      header: "Duration",
      render: (val) => (
        <span className="text-muted-foreground">
          {formatDuration(val as string)}
        </span>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="text-sm text-muted-foreground">
        <Link to="/jobs" className="text-primary hover:underline">Jobs</Link>
        <span className="mx-1">/</span>
        <span>{job.name}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-bold">{job.name}</h1>
        <StateBadge state={job.runs?.[0]?.state ?? "unknown"} />
        {job.scheduler && (
          <span className="rounded-full bg-secondary px-3 py-0.5 text-xs">
            {job.scheduler.type}: {job.scheduler.value}
          </span>
        )}
        {job.node_pool && (
          <span className="rounded-full bg-secondary px-3 py-0.5 text-xs">
            pool: {job.node_pool.name}
          </span>
        )}
      </div>

      <div className="grid grid-cols-3 gap-4">
        {/* Action Graph */}
        <div className="col-span-2 rounded-lg border overflow-hidden">
          <div className="border-b bg-muted/50 px-4 py-2.5 text-sm font-semibold">
            Action Dependency Graph
          </div>
          <div className="p-3">
            {job.action_graph && (
              <ActionGraph
                actions={job.action_graph}
                actionStates={job.runs?.[0]?.action_graph ?? undefined}
                onNodeClick={(actionName) => {
                  if (job.runs && job.runs.length > 0) {
                    navigate(`/job/${job.name}/${job.runs[0].run_num}/${actionName}`);
                  }
                }}
              />
            )}
          </div>
        </div>

        {/* Details sidebar */}
        <div className="rounded-lg border overflow-hidden self-start">
          <div className="border-b bg-muted/50 px-4 py-2 text-sm font-semibold">
            Details
          </div>
          <div className="p-3 space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-muted-foreground">Next run</span>
              <span>{formatRelativeTime(job.next_run) || "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Last success</span>
              <span className="text-green-600 dark:text-green-400">
                {formatRelativeTime(job.last_success) || "—"}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Max runtime</span>
              <span>{job.max_runtime || "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Node pool</span>
              <span>{job.node_pool?.name ?? "—"}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">Status</span>
              <span>{job.status}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Run history */}
      <div className="rounded-lg border overflow-hidden">
        <div className="flex items-center justify-between border-b bg-muted/50 px-4 py-2.5">
          <span className="text-sm font-semibold">Run History</span>
          <span className="text-xs text-muted-foreground">Auto-refreshing every 5s</span>
        </div>
        <DataTable
          columns={runColumns}
          data={job.runs ?? []}
          onRowClick={(run) => navigate(`/job/${job.name}/${run.run_num}`)}
          className="border-0 rounded-none"
        />
      </div>
    </div>
  );
}
