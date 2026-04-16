import { useCallback, useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";
import type { ActionGraphEntry, ActionRunGraphEntry, ActionState } from "@/lib/types";

cytoscape.use(dagre);

// Light mode: border-color + background by state (matches old TronWeb LESS)
const stateStyles: Record<ActionState, { border: string; bg: string; label: string }> = {
  succeeded: { border: "#218E0B", bg: "#F0FFE0", label: "Succeeded" },
  failed: { border: "#BA434F", bg: "#FFF0F0", label: "Failed" },
  running: { border: "#2F47B8", bg: "#F0F5FF", label: "Running" },
  starting: { border: "#2F47B8", bg: "#F0F5FF", label: "Starting" },
  queued: { border: "#999999", bg: "#F9F9F9", label: "Queued" },
  waiting: { border: "#999999", bg: "#F9F9F9", label: "Waiting" },
  scheduled: { border: "#999999", bg: "#F9F9F9", label: "Scheduled" },
  cancelled: { border: "#A6790D", bg: "#FFFBF0", label: "Cancelled" },
  skipped: { border: "#A6790D", bg: "#FFFBF0", label: "Skipped" },
  unknown: { border: "#D66600", bg: "#FFDBBB", label: "Unknown" },
};

// Dark mode variants
const stateStylesDark: Record<ActionState, { border: string; bg: string }> = {
  succeeded: { border: "#4ade80", bg: "#14532d" },
  failed: { border: "#f87171", bg: "#450a0a" },
  running: { border: "#60a5fa", bg: "#172554" },
  starting: { border: "#60a5fa", bg: "#172554" },
  queued: { border: "#6b7280", bg: "#1f2937" },
  waiting: { border: "#6b7280", bg: "#1f2937" },
  scheduled: { border: "#6b7280", bg: "#1f2937" },
  cancelled: { border: "#fbbf24", bg: "#422006" },
  skipped: { border: "#fbbf24", bg: "#422006" },
  unknown: { border: "#fb923c", bg: "#431407" },
};

interface ActionGraphProps {
  actions: ActionGraphEntry[];
  actionStates?: ActionRunGraphEntry[];
  onNodeClick?: (actionName: string) => void;
}

export function ActionGraph({ actions, actionStates, onNodeClick }: ActionGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const fullscreenContainerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const fullscreenTooltipRef = useRef<HTMLDivElement>(null);
  const [search, setSearch] = useState("");
  const [isFullscreen, setIsFullscreen] = useState(false);

  const isDark = typeof document !== "undefined" && document.documentElement.classList.contains("dark");

  // Build lookup for action states/commands
  const stateMap = useRef(new Map<string, ActionRunGraphEntry>()).current;
  const commandMap = useRef(new Map<string, string>()).current;

  useEffect(() => {
    stateMap.clear();
    commandMap.clear();
    if (actionStates) {
      for (const a of actionStates) {
        stateMap.set(a.name, a);
      }
    }
    for (const a of actions) {
      commandMap.set(a.name, a.command);
    }
  }, [actions, actionStates, stateMap, commandMap]);

  const buildGraph = useCallback(
    (container: HTMLElement, tooltipEl?: HTMLElement | null) => {
      const palette = isDark ? stateStylesDark : stateStyles;

      const elements: cytoscape.ElementDefinition[] = [];
      for (const action of actions) {
        const runData = stateMap.get(action.name);
        const state = runData?.state ?? "unknown";
        elements.push({
          data: {
            id: action.name,
            label: action.name,
            state,
          },
        });
        for (const dep of action.dependencies) {
          elements.push({
            data: {
              id: `${dep}->${action.name}`,
              source: dep,
              target: action.name,
            },
          });
        }
      }

      const cy = cytoscape({
        container,
        elements,
        style: [
          {
            selector: "node",
            style: {
              label: "data(label)",
              "text-valign": "center",
              "text-halign": "center",
              "background-color": ((ele: cytoscape.NodeSingular) => {
                const s = ele.data("state") as ActionState;
                return (palette[s] ?? palette.unknown).bg;
              }) as unknown as string,
              color: isDark ? "#e5e7eb" : "#000",
              "font-size": "14px",
              "font-weight": "bold",
              "text-wrap": "none",
              shape: "roundrectangle",
              width: "label",
              height: "25px",
              "padding-left": "10px",
              "padding-right": "10px",
              "border-width": 3,
              "border-color": ((ele: cytoscape.NodeSingular) => {
                const s = ele.data("state") as ActionState;
                return (palette[s] ?? palette.unknown).border;
              }) as unknown as string,
            },
          },
          {
            selector: "edge",
            style: {
              width: 2,
              "line-color": isDark ? "#4b5563" : "#999",
              "target-arrow-color": isDark ? "#4b5563" : "#999",
              "target-arrow-shape": "triangle",
              "curve-style": "bezier",
            },
          },
          {
            selector: "node.highlighted",
            style: {
              "border-width": 4,
              "overlay-opacity": 0.1,
            },
          },
          {
            selector: "edge.highlighted",
            style: {
              width: 3,
              "line-color": isDark ? "#93c5fd" : "#2F47B8",
              "target-arrow-color": isDark ? "#93c5fd" : "#2F47B8",
            },
          },
          {
            selector: "node.dimmed",
            style: {
              opacity: 0.2,
            },
          },
          {
            selector: "edge.dimmed",
            style: {
              opacity: 0.1,
            },
          },
        ],
        layout: {
          name: "dagre",
          rankDir: "LR",
          padding: 30,
          fit: true,
          nodeSep: 50,
          rankSep: 80,
        } as cytoscape.LayoutOptions,
        minZoom: 0.1,
        maxZoom: 3,
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false,
      });

      // Click handler
      if (onNodeClick) {
        cy.on("tap", "node", (evt) => {
          onNodeClick(evt.target.id());
        });
      }

      // Tooltip on hover
      cy.on("mouseover", "node", (evt) => {
        const node = evt.target;
        const tooltip = tooltipEl;
        if (!tooltip) return;

        const name = node.id();
        const runData = stateMap.get(name);
        const command = runData?.command ?? runData?.raw_command ?? commandMap.get(name) ?? "";
        const state = runData?.state ?? "unknown";
        const duration = runData?.start_time && runData?.end_time
          ? computeDuration(runData.start_time, runData.end_time)
          : null;

        tooltip.innerHTML = `
          <div class="font-semibold text-sm mb-1 break-all">${escapeHtml(name)}</div>
          <div class="flex items-center gap-2 mb-1">
            <span class="inline-block w-2.5 h-2.5 rounded-full" style="background:${(isDark ? stateStylesDark : stateStyles)[state]?.border ?? '#999'}"></span>
            <span class="text-xs">${escapeHtml(stateStyles[state]?.label ?? state)}</span>
            ${duration ? `<span class="text-xs text-muted-foreground ml-1">(${duration})</span>` : ""}
          </div>
          ${command ? `<code class="text-xs block mt-1 p-1 rounded bg-black/10 dark:bg-white/10 break-all max-h-16 overflow-hidden">${escapeHtml(command.slice(0, 200))}${command.length > 200 ? "..." : ""}</code>` : ""}
        `;
        tooltip.style.display = "block";
      });

      cy.on("mousemove", "node", (evt) => {
        const tooltip = tooltipEl;
        if (!tooltip || !container) return;
        const pos = evt.originalEvent;
        if (!pos) return;
        const rect = container.getBoundingClientRect();
        let left = pos.clientX - rect.left - tooltip.offsetWidth / 2;
        let top = pos.clientY - rect.top - tooltip.offsetHeight - 12;
        // Keep tooltip within bounds
        if (left < 0) left = 5;
        if (left + tooltip.offsetWidth > rect.width) left = rect.width - tooltip.offsetWidth - 5;
        if (top < 0) top = pos.clientY - rect.top + 20;
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
      });

      cy.on("mouseout", "node", () => {
        const tooltip = tooltipEl;
        if (tooltip) tooltip.style.display = "none";
      });

      // Path highlighting on hover
      cy.on("mouseover", "node", (evt) => {
        const node = evt.target;
        // Get all upstream (predecessors) and downstream (successors)
        const upstream = node.predecessors();
        const downstream = node.successors();
        const connected = upstream.union(downstream).union(node);

        // Dim everything
        cy.elements().addClass("dimmed");
        // Highlight connected
        connected.removeClass("dimmed").addClass("highlighted");
        node.removeClass("dimmed").addClass("highlighted");
      });

      cy.on("mouseout", "node", () => {
        cy.elements().removeClass("dimmed").removeClass("highlighted");
      });

      // Fit after dagre finishes
      setTimeout(() => {
        cy.resize();
        cy.fit(undefined, 25);
      }, 50);

      return cy;
    },
    [actions, isDark, onNodeClick, stateMap, commandMap]
  );

  // Main graph
  useEffect(() => {
    if (!containerRef.current || actions.length === 0) return;

    const cy = buildGraph(containerRef.current, tooltipRef.current);
    cyRef.current = cy;

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [actions, actionStates, buildGraph]);

  // Search filter
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    if (search) {
      const text = search.toLowerCase();
      cy.nodes().forEach((node) => {
        if (node.data("label").toLowerCase().includes(text)) {
          node.style("opacity", 1);
        } else {
          node.style("opacity", 0.2);
        }
      });
      cy.edges().style("opacity", 0.1);
      const matching = cy.nodes().filter((n) =>
        n.data("label").toLowerCase().includes(text)
      );
      matching.connectedEdges().style("opacity", 0.8);
    } else {
      cy.nodes().style("opacity", "");
      cy.edges().style("opacity", "");
    }
  }, [search]);

  function handleReset() {
    const cy = cyRef.current;
    if (!cy) return;
    setSearch("");
    cy.nodes().style("opacity", "");
    cy.edges().style("opacity", "");
    cy.elements().removeClass("dimmed").removeClass("highlighted");
    const layout = cy.layout({
      name: "dagre",
      rankDir: "LR",
      padding: 30,
      fit: true,
      nodeSep: 50,
      rankSep: 80,
    } as cytoscape.LayoutOptions);
    layout.run();
    setTimeout(() => {
      cy.fit(undefined, 25);
      cy.center();
    }, 50);
  }

  // Fullscreen graph
  useEffect(() => {
    if (!isFullscreen || !fullscreenContainerRef.current) return;

    const cy = buildGraph(fullscreenContainerRef.current, fullscreenTooltipRef.current);

    // Apply search if active
    if (search) {
      const text = search.toLowerCase();
      cy.nodes().forEach((node) => {
        if (node.data("label").toLowerCase().includes(text)) {
          node.style("opacity", 1);
        } else {
          node.style("opacity", 0.2);
        }
      });
      cy.edges().style("opacity", 0.1);
      const matching = cy.nodes().filter((n) =>
        n.data("label").toLowerCase().includes(text)
      );
      matching.connectedEdges().style("opacity", 0.8);
    }

    return () => {
      cy.destroy();
    };
  }, [isFullscreen, buildGraph, search]);

  // Unique states for legend
  const activeStates = new Set<ActionState>();
  if (actionStates) {
    for (const a of actionStates) {
      activeStates.add(a.state);
    }
  }
  if (activeStates.size === 0) {
    activeStates.add("unknown");
  }

  const graphHeight = Math.min(300, Math.max(200, actions.length * 35 + 60));

  return (
    <>
      {/* Controls */}
      <div className="flex items-center gap-2 mb-2">
        <input
          type="text"
          placeholder="Search nodes..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-7 rounded border bg-transparent px-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring w-44"
        />
        <button
          onClick={handleReset}
          className="h-7 rounded border px-2 text-xs text-muted-foreground hover:text-foreground hover:bg-muted"
          title="Reset graph position"
        >
          Reset
        </button>
        <button
          onClick={() => setIsFullscreen(true)}
          className="h-7 rounded border px-2 text-xs text-muted-foreground hover:text-foreground hover:bg-muted ml-auto"
          title="Full screen"
        >
          Expand
        </button>
      </div>

      {/* Graph */}
      <div className="relative">
        <div
          ref={containerRef}
          className="w-full rounded border bg-background [&_canvas]:cursor-pointer"
          style={{ height: graphHeight }}
        />
        {/* Tooltip */}
        <div
          ref={tooltipRef}
          className="absolute hidden z-50 rounded-md border bg-popover p-2 text-popover-foreground shadow-md max-w-sm overflow-hidden pointer-events-none"
          style={{ display: "none" }}
        />
      </div>

      {/* Legend */}
      <div className="flex flex-wrap items-center gap-3 mt-2 text-xs text-muted-foreground">
        {Array.from(activeStates).map((state) => (
          <div key={state} className="flex items-center gap-1">
            <span
              className="inline-block w-3 h-3 rounded-sm border-2"
              style={{
                borderColor: (isDark ? stateStylesDark : stateStyles)[state]?.border,
                backgroundColor: (isDark ? stateStylesDark : stateStyles)[state]?.bg,
              }}
            />
            <span>{stateStyles[state]?.label ?? state}</span>
          </div>
        ))}
      </div>

      {/* Fullscreen modal */}
      {isFullscreen && (
        <div className="fixed inset-0 z-50 flex flex-col bg-background">
          {/* Header */}
          <div className="flex items-center gap-3 border-b px-4 py-3">
            <h3 className="text-sm font-semibold">Action Dependency Graph</h3>
            <input
              type="text"
              placeholder="Search nodes..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-7 rounded border bg-transparent px-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring w-52"
            />
            <button
              onClick={handleReset}
              className="h-7 rounded border px-2 text-xs text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              Reset
            </button>
            {/* Legend inline */}
            <div className="flex items-center gap-3 ml-4 text-xs text-muted-foreground">
              {Array.from(activeStates).map((state) => (
                <div key={state} className="flex items-center gap-1">
                  <span
                    className="inline-block w-3 h-3 rounded-sm border-2"
                    style={{
                      borderColor: (isDark ? stateStylesDark : stateStyles)[state]?.border,
                      backgroundColor: (isDark ? stateStylesDark : stateStyles)[state]?.bg,
                    }}
                  />
                  <span>{stateStyles[state]?.label ?? state}</span>
                </div>
              ))}
            </div>
            <button
              onClick={() => setIsFullscreen(false)}
              className="ml-auto h-7 rounded border px-3 text-xs text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              Close
            </button>
          </div>
          {/* Fullscreen graph container */}
          <div className="flex-1 relative">
            <div
              ref={fullscreenContainerRef}
              className="absolute inset-0 [&_canvas]:cursor-pointer"
            />
            {/* Fullscreen tooltip */}
            <div
              ref={fullscreenTooltipRef}
              className="absolute hidden z-50 rounded-md border bg-popover p-2 text-popover-foreground shadow-md max-w-sm overflow-hidden pointer-events-none"
              style={{ display: "none" }}
            />
          </div>
        </div>
      )}
    </>
  );
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function computeDuration(start: string, end: string): string {
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (ms < 0) return "";
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ${secs % 60}s`;
  const hrs = Math.floor(mins / 60);
  return `${hrs}h ${mins % 60}m`;
}
