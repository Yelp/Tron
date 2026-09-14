import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatRelativeTime(timestamp: string | null): string {
  if (!timestamp) return "";
  const date = new Date(timestamp.replace(" ", "T"));
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSeconds = Math.floor(diffMs / 1000);

  if (diffSeconds < 60) return "just now";
  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

export function formatDuration(duration: string): string {
  if (!duration) return "";
  const parts = duration.split(":").map(Number);
  if (parts.length === 3) {
    const [h, m, s] = parts;
    const sf = parseFloat(s.toFixed(2));
    if (h > 0) return `${h}h ${m}m ${sf}s`;
    if (m > 0) return `${m}m ${sf}s`;
    return `${sf}s`;
  }
  if (parts.length === 2) {
    const [m, s] = parts;
    const sf = parseFloat(s.toFixed(2));
    if (m > 0) return `${m}m ${sf}s`;
    return `${sf}s`;
  }
  return duration;
}

export function formatState(state: string): string {
  return state;
}
