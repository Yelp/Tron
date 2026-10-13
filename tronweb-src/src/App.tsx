import { lazy, Suspense } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";

const Dashboard = lazy(() => import("./pages/Dashboard").then(m => ({ default: m.Dashboard })));
const JobList = lazy(() => import("./pages/JobList").then(m => ({ default: m.JobList })));
const JobDetail = lazy(() => import("./pages/JobDetail").then(m => ({ default: m.JobDetail })));
const JobRun = lazy(() => import("./pages/JobRun").then(m => ({ default: m.JobRun })));
const ActionRun = lazy(() => import("./pages/ActionRun").then(m => ({ default: m.ActionRun })));
const ConfigList = lazy(() => import("./pages/ConfigList").then(m => ({ default: m.ConfigList })));
const ConfigDetail = lazy(() => import("./pages/ConfigDetail").then(m => ({ default: m.ConfigDetail })));

function PageLoader() {
  return (
    <div className="flex items-center justify-center py-12">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
    </div>
  );
}

export function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/" element={<Navigate to="/home" replace />} />
        <Route path="/home" element={<Suspense fallback={<PageLoader />}><Dashboard /></Suspense>} />
        <Route path="/jobs" element={<Suspense fallback={<PageLoader />}><JobList /></Suspense>} />
        <Route path="/job/:name" element={<Suspense fallback={<PageLoader />}><JobDetail /></Suspense>} />
        <Route path="/job/:name/:run" element={<Suspense fallback={<PageLoader />}><JobRun /></Suspense>} />
        <Route path="/job/:name/:run/:action" element={<Suspense fallback={<PageLoader />}><ActionRun /></Suspense>} />
        <Route path="/configs" element={<Suspense fallback={<PageLoader />}><ConfigList /></Suspense>} />
        <Route path="/config/:name" element={<Suspense fallback={<PageLoader />}><ConfigDetail /></Suspense>} />
        <Route path="*" element={<Navigate to="/home" replace />} />
      </Route>
    </Routes>
  );
}
