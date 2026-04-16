import { useParams, useNavigate, Link } from "react-router-dom";
import { useJobRun } from "@/lib/queries";
import { StateBadge } from "@/components/StateBadge";
import { DataTable, type Column } from "@/components/DataTable";
import { Timeline } from "@/components/Timeline";
import { formatRelativeTime, formatDuration } from "@/lib/utils";
import type { ActionRun as ActionRunType } from "@/lib/types";

export function JobRun() {
  const { name, run } = useParams<{ name: string; run: string }>();
  const navigate = useNavigate();
  const { data: jobRun, isLoading } = useJobRun(name!, run!);

  if (isLoading || !jobRun) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  const actionColumns: Column<ActionRunType>[] = [
    {
      key: "action_name",
      header: "Action",
      render: (val) => (
        <span className="font-medium text-primary">{String(val)}</span>
      ),
    },
    {
      key: "state",
      header: "State",
      render: (val) => <StateBadge state={val as ActionRunType["state"]} />,
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
    {
      key: "exit_status",
      header: "Exit",
      render: (val) => (
        <span className="text-muted-foreground">
          {val !== null && val !== undefined ? String(val) : "—"}
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
        <Link to={`/job/${name}`} className="text-primary hover:underline">{name}</Link>
        <span className="mx-1">/</span>
        <span>#{run}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-bold">{name} #{run}</h1>
        <StateBadge state={jobRun.state} />
        <span className="text-sm text-muted-foreground">
          Duration: {formatDuration(jobRun.duration)}
        </span>
      </div>

      {/* Timeline */}
      {jobRun.runs && jobRun.runs.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div className="border-b bg-muted/50 px-4 py-2.5 text-sm font-semibold">
            Timeline
          </div>
          <div className="p-4">
            <Timeline actionRuns={jobRun.runs} />
          </div>
        </div>
      )}

      {/* Action runs table */}
      <div className="rounded-lg border overflow-hidden">
        <div className="border-b bg-muted/50 px-4 py-2.5 text-sm font-semibold">
          Action Runs
        </div>
        <DataTable
          columns={actionColumns}
          data={jobRun.runs ?? []}
          onRowClick={(action) =>
            navigate(`/job/${name}/${run}/${action.action_name}`)
          }
          className="border-0 rounded-none"
        />
      </div>
    </div>
  );
}
