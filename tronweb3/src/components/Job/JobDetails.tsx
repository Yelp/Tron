import React from 'react';
import logo from './logo.svg';
import './../../App.css';
import {formatDistanceToNow} from 'date-fns';

type Props = {
  status: string,
  node_pool: any,
  scheduler: any,
  last_success: string,
}

function JobDetails({status, node_pool, scheduler, last_success}: Props) {
  console.log(last_success)
  return (
    <table className="table-auto">
      <tbody>
        <tr>
          <td>Status</td>
          <td>{status}</td>
        </tr>
        <tr>
          <td>Node pool</td>
          <td>{node_pool?.name}</td>
        </tr>
        <tr>
          <td>Schedule</td>
          <td>{scheduler?.value}</td>
        </tr>
        <tr>
          <td>Last success</td>
          <td>{formatDistanceToNow(last_success)}</td>
        </tr>
      </tbody>
    </table>
  );
}

export default JobDetails;