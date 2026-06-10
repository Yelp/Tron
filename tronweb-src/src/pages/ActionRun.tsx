import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useActionRun } from "@/lib/queries";
import { StateBadge } from "@/components/StateBadge";
import { LogViewer } from "@/components/LogViewer";
import { formatDuration } from "@/lib/utils";

export function ActionRun() {
  const { name, run, action } = useParams<{
    name: string;
    run: string;
    action: string;
  }>();
  const [numLines, setNumLines] = useState(100);
  const [tab, setTab] = useState<"stdout" | "stderr">("stdout");
  const [commandExpanded, setCommandExpanded] = useState(false);
  const [logsExpanded, setLogsExpanded] = useState(false);

  const { data: actionRun, isLoading } = useActionRun(
    name!,
    run!,
    action!,
    numLines
  );

  if (isLoading || !actionRun) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  const isRunning = actionRun.state === "running" || actionRun.state === "starting";
  const lines = tab === "stdout" ? actionRun.stdout : actionRun.stderr;

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="text-sm text-muted-foreground">
        <Link to="/jobs" className="text-primary hover:underline">Jobs</Link>
        <span className="mx-1">/</span>
        <Link to={`/job/${name}`} className="text-primary hover:underline">{name}</Link>
        <span className="mx-1">/</span>
        <Link to={`/job/${name}/${run}`} className="text-primary hover:underline">#{run}</Link>
        <span className="mx-1">/</span>
        <span>{action}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-bold">{action}</h1>
        <StateBadge state={actionRun.state} />
        {actionRun.exit_status !== null && (
          <span className="text-sm text-muted-foreground">
            Exit code: {actionRun.exit_status}
          </span>
        )}
        <span className="text-sm text-muted-foreground">
          Duration: {formatDuration(actionRun.duration)}
        </span>
        {isRunning && (
          <span className="text-xs text-blue-600 dark:text-blue-400 animate-pulse">
            Refreshing every 2s
          </span>
        )}
      </div>

      {/* Command */}
      <div className="rounded-lg bg-slate-900 p-4">
        <div className="mb-2 flex items-center justify-between">
          <span className="text-xs font-medium uppercase text-slate-500">Command</span>
          <button
            onClick={() => setCommandExpanded(true)}
            className="text-xs text-slate-400 hover:text-slate-200"
          >
            Expand
          </button>
        </div>
        <div className="max-h-32 overflow-auto">
          <code className="text-sm text-slate-200 break-all">
            {actionRun.command}
          </code>
        </div>
      </div>

      {/* Command fullscreen */}
      {commandExpanded && (
        <div className="fixed inset-0 z-50 flex flex-col bg-slate-900">
          <div className="flex items-center justify-between border-b border-slate-700 px-4 py-3">
            <span className="text-sm font-semibold text-slate-200">Command</span>
            <button
              onClick={() => setCommandExpanded(false)}
              className="rounded border border-slate-600 px-3 py-1 text-xs text-slate-400 hover:text-slate-200 hover:bg-slate-800"
            >
              Close
            </button>
          </div>
          <div className="flex-1 overflow-auto p-4">
            <code className="text-sm text-slate-200 break-all whitespace-pre-wrap">
              {actionRun.command}
            </code>
          </div>
        </div>
      )}

      {/* Log viewer with tabs */}
      <div>
        <div className="mb-2 flex items-center gap-1">
          <button
            onClick={() => setTab("stdout")}
            className={`rounded-md px-3 py-1 text-sm ${
              tab === "stdout"
                ? "bg-secondary font-medium text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            stdout
            {actionRun.stdout && (
              <span className="ml-1 text-xs text-muted-foreground">
                ({actionRun.stdout.length})
              </span>
            )}
          </button>
          <button
            onClick={() => setTab("stderr")}
            className={`rounded-md px-3 py-1 text-sm ${
              tab === "stderr"
                ? "bg-secondary font-medium text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            stderr
            {actionRun.stderr && (
              <span className="ml-1 text-xs text-muted-foreground">
                ({actionRun.stderr.length})
              </span>
            )}
          </button>
          <button
            onClick={() => setLogsExpanded(true)}
            className="ml-auto rounded border px-2 py-1 text-xs text-muted-foreground hover:text-foreground hover:bg-muted"
          >
            Expand
          </button>
        </div>

        <LogViewer
          lines={lines ?? []}
          isStreaming={isRunning}
          onLoadFull={
            numLines < 10000
              ? () => setNumLines(10000)
              : undefined
          }
        />
      </div>

      {/* Logs fullscreen */}
      {logsExpanded && (
        <div className="fixed inset-0 z-50 flex flex-col bg-background">
          <div className="flex items-center gap-3 border-b px-4 py-3">
            <div className="flex items-center gap-1">
              <button
                onClick={() => setTab("stdout")}
                className={`rounded-md px-3 py-1 text-sm ${
                  tab === "stdout"
                    ? "bg-secondary font-medium text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
              >
                stdout
                {actionRun.stdout && (
                  <span className="ml-1 text-xs text-muted-foreground">
                    ({actionRun.stdout.length})
                  </span>
                )}
              </button>
              <button
                onClick={() => setTab("stderr")}
                className={`rounded-md px-3 py-1 text-sm ${
                  tab === "stderr"
                    ? "bg-secondary font-medium text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
              >
                stderr
                {actionRun.stderr && (
                  <span className="ml-1 text-xs text-muted-foreground">
                    ({actionRun.stderr.length})
                  </span>
                )}
              </button>
            </div>
            <button
              onClick={() => setLogsExpanded(false)}
              className="ml-auto rounded border px-3 py-1 text-xs text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              Close
            </button>
          </div>
          <div className="flex-1 overflow-hidden">
            <LogViewer
              lines={lines ?? []}
              isStreaming={isRunning}
              fullHeight
              className="h-full border-0 rounded-none"
              onLoadFull={
                numLines < 10000
                  ? () => setNumLines(10000)
                  : undefined
              }
            />
          </div>
        </div>
      )}
    </div>
  );
}
