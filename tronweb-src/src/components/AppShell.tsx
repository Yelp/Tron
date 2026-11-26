import { Outlet, NavLink, useLocation } from "react-router-dom";
import { useState } from "react";
import { ThemeToggle } from "./ThemeToggle";
import { CommandPalette } from "./CommandPalette";
import { useStatus } from "@/lib/queries";
import { cn } from "@/lib/utils";

const navItems = [
  { path: "/home", label: "Dashboard", prefetch: () => import("@/pages/Dashboard") },
  { path: "/jobs", label: "Jobs", prefetch: () => import("@/pages/JobList") },
  { path: "/configs", label: "Config", prefetch: () => import("@/pages/ConfigList") },
];

export function AppShell() {
  const [commandOpen, setCommandOpen] = useState(false);
  const { data: status } = useStatus();
  const location = useLocation();

  function isActive(path: string) {
    if (path === "/") return location.pathname === "/";
    return location.pathname.startsWith(path);
  }

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-50 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="flex h-14 items-center px-6">
          <NavLink to="/" className="mr-8 text-lg font-bold">
            TronWeb
          </NavLink>

          <nav className="flex items-center gap-1">
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                onMouseEnter={() => item.prefetch()}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm transition-colors",
                  isActive(item.path)
                    ? "bg-secondary font-medium text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>

          <div className="ml-auto flex items-center gap-3">
            <button
              onClick={() => setCommandOpen(true)}
              className="inline-flex h-8 items-center gap-2 rounded-md border bg-background px-3 text-xs text-muted-foreground hover:bg-accent"
            >
              <span>Search jobs...</span>
              <kbd className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-mono">
                Ctrl+K
              </kbd>
            </button>
            <ThemeToggle />
            {status && (
              <span className="text-xs text-muted-foreground">
                v{status.version}
              </span>
            )}
          </div>
        </div>
      </header>

      <main className="p-6">
        <Outlet />
      </main>

      <CommandPalette open={commandOpen} onOpenChange={setCommandOpen} />
    </div>
  );
}
