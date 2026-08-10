import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import type { ActionRun } from "./types";

export function useStatus() {
  return useQuery({
    queryKey: ["status"],
    queryFn: api.getStatus,
    refetchInterval: 30_000,
  });
}

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: api.getJobs,
    refetchInterval: 10_000,
  });
}

export function useJob(name: string) {
  return useQuery({
    queryKey: ["job", name],
    queryFn: () => api.getJob(name),
    refetchInterval: 5_000,
    enabled: !!name,
  });
}

export function useJobRun(jobName: string, runNum: string) {
  return useQuery({
    queryKey: ["jobRun", jobName, runNum],
    queryFn: () => api.getJobRun(jobName, runNum),
    refetchInterval: 5_000,
    enabled: !!jobName && !!runNum,
  });
}

export function useActionRun(
  jobName: string,
  runNum: string,
  actionName: string,
  numLines: number = 100
) {
  return useQuery({
    queryKey: ["actionRun", jobName, runNum, actionName, numLines],
    queryFn: () => api.getActionRun(jobName, runNum, actionName, numLines),
    refetchInterval: (query) => {
      const data = query.state.data as ActionRun | undefined;
      if (data?.state === "running" || data?.state === "starting") {
        return 2_000;
      }
      return false;
    },
    enabled: !!jobName && !!runNum && !!actionName,
  });
}

export function useConfigs() {
  return useQuery({
    queryKey: ["configs"],
    queryFn: api.getConfigs,
  });
}

export function useConfig(name: string) {
  return useQuery({
    queryKey: ["config", name],
    queryFn: () => api.getConfig(name),
    enabled: !!name,
  });
}

export function useApiIndex() {
  return useQuery({
    queryKey: ["index"],
    queryFn: api.getIndex,
    staleTime: 60_000,
  });
}
