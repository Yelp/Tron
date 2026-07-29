import { useEffect, useState } from "react";
import { Moon, Sun } from "lucide-react";

function getSystemDark() {
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

export function ThemeToggle() {
  const [dark, setDark] = useState(() => {
    if (typeof window === "undefined") return false;
    const stored = localStorage.getItem("tronweb-theme");
    if (stored) return stored === "dark";
    return getSystemDark();
  });

  useEffect(() => {
    const root = document.documentElement;
    if (dark) {
      root.classList.add("dark");
    } else {
      root.classList.remove("dark");
    }
    localStorage.setItem("tronweb-theme", dark ? "dark" : "light");
  }, [dark]);

  // Follow system preference changes
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  return (
    <button
      onClick={() => setDark(!dark)}
      className="inline-flex h-8 w-8 items-center justify-center rounded-md bg-secondary text-secondary-foreground hover:bg-accent"
      aria-label="Toggle theme"
    >
      {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
    </button>
  );
}
