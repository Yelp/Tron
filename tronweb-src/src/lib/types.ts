export type ActionState =
  | "scheduled"
  | "queued"
  | "waiting"
  | "starting"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "skipped"
  | "unknown";

export type JobStatus = "enabled" | "disabled";

export interface Scheduler {
  value: string;
  type: string;
  jitter: string | null;
}

export interface Node {
  name: string;
  hostname: string;
  username: string;
  port: number;
}

export interface NodePool {
  name: string;
  nodes: Node[];
}

export interface ActionGraphEntry {
  name: string;
  command: string;
  dependencies: string[];
}

export interface ActionRunGraphEntry {
  id: string;
  name: string;
  command: string;
  raw_command: string;
  state: ActionState;
  start_time: string | null;
  end_time: string | null;
  dependencies: string[];
}

export interface ActionRun {
  id: string;
  action_name: string;
  state: ActionState;
  start_time: string | null;
  end_time: string | null;
  exit_status: number | null;
  exit_statuses: (number | null)[];
  command: string;
  raw_command: string;
  original_command: string;
  retries_remaining: number;
  retries_delay: string | null;
  in_delay: number | null;
  node: Node;
  duration: string;
  job_name: string;
  run_num: string;
  requirements: string[] | null;
  stdout: string[] | null;
  stderr: string[] | null;
  meta: string[] | null;
  triggered_by: string;
  trigger_downstreams: string;
}

export interface JobRun {
  id: string;
  run_num: number;
  run_time: string;
  start_time: string | null;
  end_time: string | null;
  manual: boolean;
  job_name: string;
  state: ActionState;
  node: Node;
  duration: string;
  url: string;
  runs: ActionRun[] | null;
  action_graph: ActionRunGraphEntry[] | null;
}

export interface Job {
  name: string;
  status: JobStatus;
  all_nodes: boolean;
  allow_overlap: boolean;
  queueing: boolean;
  scheduler: Scheduler;
  action_names: string[];
  node_pool: NodePool | null;
  last_success: string | null;
  next_run: string | null;
  url: string;
  runs: JobRun[] | null;
  max_runtime: string;
  action_graph: ActionGraphEntry[] | null;
  monitoring: Record<string, unknown> | null;
  expected_runtime: number | null;
}

export interface JobsResponse {
  jobs: Job[];
}

export interface ConfigNamespace {
  name: string;
  [key: string]: unknown;
}

export interface Config {
  config: string;
  hash: string;
}

export interface StatusResponse {
  status: string;
  version: string;
  boot_time: number;
}

export interface ApiIndex {
  jobs: Record<string, ActionGraphEntry[]>;
  namespaces: Record<string, unknown>;
}
