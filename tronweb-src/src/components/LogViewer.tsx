import { useState, useRef, useEffect, useMemo } from "react";
import { Search } from "lucide-react";
import { cn } from "@/lib/utils";

interface LogViewerProps {
  lines: string[];
  isStreaming?: boolean;
  onLoadFull?: () => void;
  className?: string;
  fullHeight?: boolean;
}

function colorize(line: string): React.ReactNode {
  if (line.includes("ERROR") || line.includes("Traceback") || line.includes("Exception")) {
    return <span className="text-red-400">{line}</span>;
  }
  if (line.includes("WARN")) {
    return <span className="text-yellow-400">{line}</span>;
  }
  if (line.includes("INFO")) {
    return <span className="text-blue-300">{line}</span>;
  }
  return <span className="text-slate-300">{line}</span>;
}

export function LogViewer({ lines, isStreaming, onLoadFull, className, fullHeight }: LogViewerProps) {
  const [search, setSearch] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const [VirtuosoComponent, setVirtuosoComponent] = useState<any>(null);
  const virtuosoRef = useRef<any>(null);

  // Dynamically load react-virtuoso
  useEffect(() => {
    import("react-virtuoso").then((m) => {
      setVirtuosoComponent(() => m.Virtuoso);
    });
  }, []);

  const filteredLines = useMemo(() => {
    if (!search) return lines.map((line, i) => ({ line, num: i + 1 }));
    const q = search.toLowerCase();
    return lines
      .map((line, i) => ({ line, num: i + 1 }))
      .filter(({ line }) => line.toLowerCase().includes(q));
  }, [lines, search]);

  useEffect(() => {
    if (autoScroll && isStreaming && virtuosoRef.current) {
      virtuosoRef.current.scrollToIndex({
        index: filteredLines.length - 1,
        behavior: "smooth",
      });
    }
  }, [filteredLines.length, autoScroll, isStreaming]);

  const Virtuoso = VirtuosoComponent;

  return (
    <div className={cn("rounded-lg border overflow-hidden", fullHeight && "flex flex-col h-full", className)}>
      {/* Toolbar */}
      <div className="flex items-center gap-3 border-b bg-muted/50 px-4 py-2">
        <span className="text-sm font-semibold">Output</span>
        <div className="relative ml-4">
          <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search logs..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-7 rounded-md border bg-background pl-7 pr-3 text-xs outline-none focus:ring-1 focus:ring-ring w-48"
          />
        </div>
        <div className="ml-auto flex items-center gap-3">
          {onLoadFull && (
            <button
              onClick={onLoadFull}
              className="text-xs text-primary hover:underline"
            >
              Load full log
            </button>
          )}
          {isStreaming && (
            <button
              onClick={() => setAutoScroll(!autoScroll)}
              className={cn(
                "text-xs",
                autoScroll ? "text-green-600 dark:text-green-400" : "text-muted-foreground"
              )}
            >
              {autoScroll ? "Auto-scroll on" : "Auto-scroll off"}
            </button>
          )}
          <span className="text-xs text-muted-foreground">
            {filteredLines.length} lines
          </span>
        </div>
      </div>

      {/* Log content */}
      <div className={cn("bg-slate-900", fullHeight ? "flex-1" : "")} style={fullHeight ? undefined : { height: Math.min(400, filteredLines.length * 20 + 24) }}>
        {Virtuoso ? (
          <Virtuoso
            ref={virtuosoRef}
            data={filteredLines}
            style={{ height: "100%" }}
            itemContent={(_: number, { line, num }: { line: string; num: number }) => (
              <div className="flex px-4 py-0 font-mono text-xs leading-5 hover:bg-slate-800/50">
                <span className="mr-4 w-8 select-none text-right text-slate-600">
                  {num}
                </span>
                {colorize(line)}
              </div>
            )}
            followOutput={autoScroll && isStreaming ? "smooth" : false}
            atBottomStateChange={(atBottom: boolean) => {
              if (isStreaming && atBottom) setAutoScroll(true);
            }}
          />
        ) : (
          <div className="flex items-center justify-center h-full text-xs text-slate-500">
            Loading viewer...
          </div>
        )}
      </div>
    </div>
  );
}
