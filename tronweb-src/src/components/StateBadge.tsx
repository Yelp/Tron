import { memo } from "react";
import { cn } from "@/lib/utils";
import type { ActionState } from "@/lib/types";

const stateStyles: Record<ActionState, string> = {
  succeeded: "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-400",
  failed: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-400",
  running: "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-400",
  starting: "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-400",
  queued: "bg-yellow-100 text-yellow-800 dark:bg-yellow-950 dark:text-yellow-400",
  waiting: "bg-yellow-100 text-yellow-800 dark:bg-yellow-950 dark:text-yellow-400",
  scheduled: "bg-indigo-100 text-indigo-800 dark:bg-indigo-950 dark:text-indigo-400",
  cancelled: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
  skipped: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
  unknown: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
};

interface StateBadgeProps {
  state: ActionState;
  className?: string;
}

export const StateBadge = memo(function StateBadge({ state, className }: StateBadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium",
        stateStyles[state] ?? stateStyles.unknown,
        className
      )}
    >
      {state}
    </span>
  );
});
