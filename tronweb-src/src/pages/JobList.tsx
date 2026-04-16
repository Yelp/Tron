import { useState, useMemo } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useJobs } from "@/lib/queries";
import { DataTable, type Column } from "@/components/DataTable";
import { StateBadge } from "@/components/StateBadge";
import { formatRelativeTime } from "@/lib/utils";
import type { Job, ActionState } from "@/lib/types";
import { Search } from "lucide-react";

function getLatestRunState(job: Job): ActionState {
  if (job.runs && job.runs.length > 0) {
    return job.runs[0].state;
  }
  return "unknown";
}

export function JobList() {
  const { data: jobs, isLoading } = useJobs();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState(searchParams.get("q") ?? "");
  const [stateFilter, setStateFilter] = useState(searchParams.get("state") ?? "");

  const filteredJobs = useMemo(() => {
    if (!jobs) return [];
    let result = jobs;
    if (query) {
      const q = query.toLowerCase();
      result = result.filter((j) => j.name.toLowerCase().includes(q));
    }
    if (stateFilter) {
      result = result.filter((j) => getLatestRunState(j) === stateFilter);
    }
    return result;
  }, [jobs, query, stateFilter]);

  function handleQueryChange(value: string) {
    setQuery(value);
    const params = new URLSearchParams(searchParams);
    if (value) params.set("q", value);
    else params.delete("q");
    setSearchParams(params, { replace: true });
  }

  function handleStateChange(value: string) {
    setStateFilter(value);
    const params = new URLSearchParams(searchParams);
    if (value) params.set("state", value);
    else params.delete("state");
    setSearchParams(params, { replace: true });
  }

  const columns: Column<Job>[] = [
    {
      key: "name",
      header: "Job Name",
      render: (_, row) => (
        <span className="font-medium text-primary">{row.name}</span>
      ),
    },
    {
      key: "status",
      header: "Status",
      render: (_, row) => <StateBadge state={getLatestRunState(row)} />,
    },
    {
      key: "last_success",
      header: "Last Success",
      render: (val) => (
        <span className="text-muted-foreground">
          {formatRelativeTime(val as string | null)}
        </span>
      ),
    },
    {
      key: "next_run",
      header: "Next Run",
      render: (val) => (
        <span className="text-muted-foreground">
          {formatRelativeTime(val as string | null)}
        </span>
      ),
    },
  ];

  if (isLoading) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            placeholder="Filter by name..."
            value={query}
            onChange={(e) => handleQueryChange(e.target.value)}
            className="h-9 w-full rounded-md border bg-background pl-9 pr-3 text-sm outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
        <select
          value={stateFilter}
          onChange={(e) => handleStateChange(e.target.value)}
          className="h-9 rounded-md border bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="">All states</option>
          <option value="succeeded">Succeeded</option>
          <option value="failed">Failed</option>
          <option value="running">Running</option>
          <option value="queued">Queued</option>
          <option value="scheduled">Scheduled</option>
        </select>
        <span className="text-xs text-muted-foreground">
          {filteredJobs.length} jobs
        </span>
      </div>

      <DataTable
        columns={columns}
        data={filteredJobs}
        onRowClick={(job) => navigate(`/job/${job.name}`)}
      />
    </div>
  );
}
