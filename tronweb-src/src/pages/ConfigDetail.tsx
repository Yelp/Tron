import { useParams, Link } from "react-router-dom";
import { useConfig } from "@/lib/queries";

export function ConfigDetail() {
  const { name } = useParams<{ name: string }>();
  const { data: config, isLoading } = useConfig(name!);

  if (isLoading || !config) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  return (
    <div className="space-y-4">
      {/* Breadcrumb */}
      <div className="text-sm text-muted-foreground">
        <Link to="/configs" className="text-primary hover:underline">Config</Link>
        <span className="mx-1">/</span>
        <span>{name}</span>
      </div>

      <h1 className="text-xl font-bold">{name}</h1>

      {/* Config content */}
      <div className="rounded-lg border overflow-hidden">
        <div className="flex items-center justify-between border-b bg-muted/50 px-4 py-2.5">
          <span className="text-sm font-semibold">YAML Configuration</span>
          <span className="text-xs text-muted-foreground font-mono">
            hash: {config.hash?.slice(0, 8)}
          </span>
        </div>
        <pre className="overflow-auto bg-slate-900 p-4 text-sm text-slate-200 font-mono leading-relaxed">
          {config.config}
        </pre>
      </div>
    </div>
  );
}
