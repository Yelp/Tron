import React,
{
  useState,
  useEffect,
} from 'react';
import { Link } from 'react-router-dom';
import { getJobColor, fetchFromApi } from '../../utils/utils';
import JobScheduler from '../JobScheduler';

function buildJobTable(jobData) {
  const tableRows = jobData.jobs.map((job) => (
    <tr key={job.name} className="clickable">
      <td>
        <Link className="text-dark" to={`job/${job.name}`}>{job.name}</Link>
      </td>
      <td className={`text-${getJobColor(job.status)}`}>{job.status}</td>
      <td><JobScheduler scheduler={job.scheduler} /></td>
      <td>{job.node_pool.name}</td>
      <td>{job.last_success}</td>
      <td>{job.next_run}</td>
    </tr>
  ));
  return (
    <table className="table table-hover">
      <thead className="thead-light">
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Schedule</th>
          <th>Node pool</th>
          <th>Last success</th>
          <th>Next run</th>
        </tr>
      </thead>
      <tbody>
        {tableRows}
      </tbody>
    </table>
  );
}

function JobsDashboard() {
  const [jobData, setJobData] = useState(undefined);

  useEffect(() => {
    fetchFromApi('/api/jobs', setJobData);
  }, []);

  let jobContent = (
    <div className="spinner-border" role="status">
      <span className="sr-only">Loading...</span>
    </div>
  );
  if (jobData !== undefined) {
    jobContent = buildJobTable(jobData);
  }
  return (
    <div>
      <h1>Scheduled Jobs</h1>
      {jobContent}
    </div>
  );
}

export default JobsDashboard;
