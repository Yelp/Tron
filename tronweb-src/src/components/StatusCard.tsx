import { cn } from "@/lib/utils";

interface StatusCardProps {
  label: string;
  count: number;
  variant: "success" | "danger" | "info" | "warning" | "purple";
  onClick?: () => void;
}

const variantStyles = {
  success: "bg-green-50 border-green-200 dark:bg-green-950/30 dark:border-green-900",
  danger: "bg-red-50 border-red-200 dark:bg-red-950/30 dark:border-red-900",
  info: "bg-blue-50 border-blue-200 dark:bg-blue-950/30 dark:border-blue-900",
  warning: "bg-yellow-50 border-yellow-200 dark:bg-yellow-950/30 dark:border-yellow-900",
  purple: "bg-indigo-50 border-indigo-200 dark:bg-indigo-950/30 dark:border-indigo-900",
};

const countStyles = {
  success: "text-green-600 dark:text-green-400",
  danger: "text-red-600 dark:text-red-400",
  info: "text-blue-600 dark:text-blue-400",
  warning: "text-yellow-600 dark:text-yellow-400",
  purple: "text-indigo-600 dark:text-indigo-400",
};

export function StatusCard({ label, count, variant, onClick }: StatusCardProps) {
  return (
    <div
      className={cn(
        "flex-1 rounded-lg border p-4 transition-colors",
        variantStyles[variant],
        onClick && "cursor-pointer hover:opacity-80"
      )}
      onClick={onClick}
    >
      <div className="text-xs font-medium uppercase text-muted-foreground">
        {label}
      </div>
      <div className={cn("mt-1 text-2xl font-bold", countStyles[variant])}>
        {count}
      </div>
    </div>
  );
}
