import type {
  Job,
  JobRun,
  ActionRun,
  JobsResponse,
  Config,
  ConfigNamespace,
  StatusResponse,
  ApiIndex,
} from "./types";

const API_BASE = "/api";

class ApiError extends Error {
  constructor(public status: number, statusText: string) {
    super(`${status} ${statusText}`);
    this.name = "ApiError";
  }
}

function buildUrl(path: string, params?: Record<string, string | number>): string {
  const url = `${API_BASE}${path}`;
  if (!params) return url;
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    searchParams.set(key, String(value));
  }
  return `${url}?${searchParams.toString()}`;
}

async function get<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = buildUrl(path, params);
  const response = await fetch(url);
  if (!response.ok) {
    throw new ApiError(response.status, response.statusText);
  }
  return response.json();
}

export const api = {
  getIndex(): Promise<ApiIndex> {
    return get<ApiIndex>("/");
  },

  async getJobs(): Promise<Job[]> {
    const response = await get<JobsResponse>("/jobs", { include_job_runs: 1 });
    return response.jobs;
  },

  getJob(name: string): Promise<Job> {
    return get<Job>(`/jobs/${name}`, {
      include_action_graph: 1,
      include_job_runs: 1,
    });
  },

  getJobRun(jobName: string, runNum: string): Promise<JobRun> {
    return get<JobRun>(`/jobs/${jobName}/${runNum}`, {
      include_action_runs: 1,
      include_action_graph: 1,
    });
  },

  getActionRun(
    jobName: string,
    runNum: string,
    actionName: string,
    numLines: number = 100
  ): Promise<ActionRun> {
    return get<ActionRun>(`/jobs/${jobName}/${runNum}/${actionName}`, {
      num_lines: numLines,
      include_stdout: 1,
      include_stderr: 1,
    });
  },

  async getConfigs(): Promise<ConfigNamespace[]> {
    const response = await get<Record<string, Config>>("/config");
    return Object.entries(response).map(([name, config]) => ({
      name,
      ...config,
    }));
  },

  getConfig(name: string): Promise<Config> {
    return get<Config>("/config", { name });
  },

  getStatus(): Promise<StatusResponse> {
    return get<StatusResponse>("/status");
  },
};
