import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Command } from "cmdk";
import { useApiIndex } from "@/lib/queries";

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CommandPalette({ open, onOpenChange }: CommandPaletteProps) {
  const navigate = useNavigate();
  const { data: index } = useApiIndex();
  const [search, setSearch] = useState("");

  // Ctrl+K handler
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        onOpenChange(!open);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [open, onOpenChange]);

  const jobNames = useMemo(() => {
    if (!index?.jobs) return [];
    return Object.keys(index.jobs);
  }, [index]);

  const namespaces = useMemo(() => {
    if (!index?.namespaces) return [];
    return Object.keys(index.namespaces);
  }, [index]);

  function selectJob(name: string) {
    navigate(`/job/${name}`);
    onOpenChange(false);
    setSearch("");
  }

  function selectConfig(name: string) {
    navigate(`/config/${name}`);
    onOpenChange(false);
    setSearch("");
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={() => onOpenChange(false)}
      />

      {/* Dialog */}
      <div className="absolute left-1/2 top-[20%] w-full max-w-lg -translate-x-1/2">
        <Command
          className="rounded-lg border bg-popover text-popover-foreground shadow-lg overflow-hidden"
          shouldFilter={true}
        >
          <div className="flex items-center border-b px-3">
            <Command.Input
              value={search}
              onValueChange={setSearch}
              placeholder="Search jobs and configs..."
              className="flex h-11 w-full bg-transparent py-3 text-sm outline-none placeholder:text-muted-foreground"
              autoFocus
            />
          </div>
          <Command.List className="max-h-80 overflow-y-auto p-2">
            <Command.Empty className="py-6 text-center text-sm text-muted-foreground">
              No results found.
            </Command.Empty>

            <Command.Group heading="Jobs" className="text-xs font-medium text-muted-foreground px-2 py-1.5">
              {jobNames.map((name) => (
                <Command.Item
                  key={name}
                  value={name}
                  onSelect={() => selectJob(name)}
                  className="flex cursor-pointer items-center rounded-md px-2 py-1.5 text-sm aria-selected:bg-accent"
                >
                  {name}
                </Command.Item>
              ))}
            </Command.Group>

            <Command.Group heading="Configs" className="text-xs font-medium text-muted-foreground px-2 py-1.5">
              {namespaces.map((name) => (
                <Command.Item
                  key={name}
                  value={`config:${name}`}
                  onSelect={() => selectConfig(name)}
                  className="flex cursor-pointer items-center rounded-md px-2 py-1.5 text-sm aria-selected:bg-accent"
                >
                  {name}
                </Command.Item>
              ))}
            </Command.Group>
          </Command.List>
        </Command>
      </div>
    </div>
  );
}
