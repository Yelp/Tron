import React,
{
  useState,
  useEffect,
} from 'react';
import Fuse from 'fuse.js';
import { getJobColor, fetchFromApi } from '../../utils/utils';
import JobScheduler from '../JobScheduler';
import './JobsDashboard.css';

function buildJobTable(jobsList) {
  const tableRows = jobsList.map((job) => (
    <tr key={job.name} onClick={() => window.location.assign(`#/job/${job.name}`)}>
      <td className="name-cell">{job.name}</td>
      <td className={`text-${getJobColor(job.status)}`}>{job.status}</td>
      <td><JobScheduler scheduler={job.scheduler} /></td>
      <td>{job.node_pool.name}</td>
      <td>{job.last_success}</td>
      <td>{job.next_run}</td>
    </tr>
  ));
  return (
    <table className="table table-hover table-responsive">
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
  const [inputValue, setInputValue] = useState('');

  useEffect(() => fetchFromApi('/api/jobs', setJobData), []);

  let jobContent = (
    <div className="spinner-border" role="status">
      <span className="sr-only">Loading...</span>
    </div>
  );

  function filterJobs(string) {
    // Require close to exact match, but allow anywhere in the substring
    const searchOptions = { keys: ['name'], threshold: 0.2, ignoreLocation: true };
    const fuseSearch = new Fuse(jobData.jobs, searchOptions);
    return fuseSearch.search(string).map((result) => result.item);
  }

  if (jobData !== undefined) {
    if ('error' in jobData) {
      jobContent = (
        <p>
          Error:
          {jobData.error.message}
        </p>
      );
    } else {
      let jobsListToShow = jobData.jobs;
      if (inputValue !== '') {
        jobsListToShow = filterJobs(inputValue);
      }
      jobContent = (
        <div>
          <div className="mb-3">
            <input type="text" className="form-control" placeholder="Search jobs" onInput={(e) => setInputValue(e.target.value)} />
          </div>
          {buildJobTable(jobsListToShow)}
        </div>
      );
    }
  }
  return (
    <div>
      <h1>Scheduled Jobs</h1>
      {jobContent}
    </div>
  );
}

export default JobsDashboard;
