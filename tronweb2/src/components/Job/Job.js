import React,
{
  useState,
  useEffect,
} from 'react';
import {
  useParams,
} from 'react-router-dom';
import JobScheduler from '../JobScheduler';
import JobSettings from '../JobSettings';
import ActionGraph from '../ActionGraph';
import { getJobColor, fetchFromApi } from '../../utils/utils';

function jobDisplay(job) {
  return (
    <div className="row">
      <div className="col-md-5">
        <h2>Details</h2>
        <div>
          <table className="table details">
            <tbody>
              <tr>
                <td>Status</td>
                <td className={`text-${getJobColor(job.status)}`}>{job.status}</td>
              </tr>
              <tr>
                <td>Node pool</td>
                <td>{job.node_pool.name}</td>
              </tr>
              <tr>
                <td>Schedule</td>
                <td><JobScheduler scheduler={job.scheduler} /></td>
              </tr>
              <tr>
                <td>Settings</td>
                <td>
                  <JobSettings
                    allowOverlap={job.allow_overlap}
                    queueing={job.queueing}
                    allNodes={job.all_nodes}
                  />
                </td>
              </tr>
              <tr>
                <td>Last success</td>
                <td>{job.last_success}</td>
              </tr>
              <tr>
                <td>Next run</td>
                <td>{job.next_run}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div className="col-md-7">
        <h2>Action Graph</h2>
        <ActionGraph actionData={job.action_graph} height={450} width={600} />
      </div>
    </div>
  );
}

function Job() {
  const [job, setJobData] = useState(undefined);
  const { jobId } = useParams();

  useEffect(() => {
    document.title = jobId;
    return fetchFromApi(`/api/jobs/${jobId}?include_action_graph=1`, setJobData);
  }, [jobId]);

  let jobContent = (
    <div className="spinner-border" role="status">
      <span className="sr-only">Loading...</span>
    </div>
  );
  if (job !== undefined) {
    if ('error' in job) {
      jobContent = (
        <p>
          Error:
          {job.error.message}
        </p>
      );
    } else {
      jobContent = jobDisplay(job);
    }
  }

  return (
    <div>
      <h1>{jobId}</h1>
      {jobContent}
    </div>
  );
}

export default Job;
