import { useEffect, useRef, useState } from "react";
import type { ActionRun } from "@/lib/types";

const stateColors: Record<string, string> = {
  succeeded: "#16a34a",
  failed: "#dc2626",
  running: "#2563eb",
  starting: "#2563eb",
  queued: "#ca8a04",
  waiting: "#ca8a04",
  scheduled: "#4f46e5",
  cancelled: "#6b7280",
  skipped: "#6b7280",
  unknown: "#9ca3af",
};

interface D3Deps {
  select: typeof import("d3-selection").select;
  scaleTime: typeof import("d3-scale").scaleTime;
  axisBottom: typeof import("d3-axis").axisBottom;
  extent: typeof import("d3-array").extent;
}

interface TimelineProps {
  actionRuns: ActionRun[];
}

export function Timeline({ actionRuns }: TimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [d3, setD3] = useState<D3Deps | null>(null);

  // Only import the d3 modules we actually use
  useEffect(() => {
    Promise.all([
      import("d3-selection"),
      import("d3-scale"),
      import("d3-axis"),
      import("d3-array"),
    ]).then(([selection, scale, axis, array]) => {
      setD3({
        select: selection.select,
        scaleTime: scale.scaleTime,
        axisBottom: axis.axisBottom,
        extent: array.extent,
      });
    });
  }, []);

  useEffect(() => {
    if (!containerRef.current || !d3 || actionRuns.length === 0) return;

    const runs = actionRuns.filter((r) => r.start_time);
    if (runs.length === 0) return;

    // Measure longest action name to size the left margin
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d")!;
    ctx.font = "11px sans-serif";
    const maxLabelWidth = Math.max(
      80,
      ...runs.map((r) => ctx.measureText(r.action_name).width)
    );
    const margin = { top: 10, right: 20, bottom: 30, left: Math.ceil(maxLabelWidth) + 12 };
    const width = containerRef.current.clientWidth - margin.left - margin.right;
    const barHeight = 24;
    const barGap = 4;
    const height = runs.length * (barHeight + barGap);

    d3.select(containerRef.current).selectAll("*").remove();

    const svg = d3
      .select(containerRef.current)
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const now = new Date();
    const allTimes = runs.flatMap((r) => {
      const times: Date[] = [new Date(r.start_time!.replace(" ", "T"))];
      if (r.end_time) times.push(new Date(r.end_time.replace(" ", "T")));
      else times.push(now);
      return times;
    });
    const timeExtent = d3.extent(allTimes) as [Date, Date];

    const xScale = d3.scaleTime().domain(timeExtent).range([0, width]);
    const xAxis = d3.axisBottom(xScale).ticks(5);

    svg
      .append("g")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis)
      .selectAll("text")
      .style("font-size", "10px");

    runs.forEach((run, i) => {
      const start = new Date(run.start_time!.replace(" ", "T"));
      const end = run.end_time ? new Date(run.end_time.replace(" ", "T")) : now;
      const y = i * (barHeight + barGap);

      svg
        .append("rect")
        .attr("x", xScale(start))
        .attr("y", y)
        .attr("width", Math.max(2, xScale(end) - xScale(start)))
        .attr("height", barHeight)
        .attr("rx", 4)
        .attr("fill", stateColors[run.state] ?? "#9ca3af")
        .attr("opacity", 0.85);

      svg
        .append("text")
        .attr("x", -4)
        .attr("y", y + barHeight / 2)
        .attr("text-anchor", "end")
        .attr("dominant-baseline", "middle")
        .style("font-size", "11px")
        .style("fill", "currentColor")
        .text(run.action_name);
    });
  }, [actionRuns, d3]);

  if (actionRuns.filter((r) => r.start_time).length === 0) {
    return <div className="text-sm text-muted-foreground">No timing data available</div>;
  }

  if (!d3) {
    return <div className="text-sm text-muted-foreground">Loading timeline...</div>;
  }

  return <div ref={containerRef} className="w-full overflow-x-auto" />;
}
