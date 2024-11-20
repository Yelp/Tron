import React, { useState } from 'react';
import logo from './logo.svg';
import './App.css';
import { useQuery, useQueryClient } from '@tanstack/react-query';

type CardProps = {
  name: string,
  status: string,
  runNumber: number,
  href: string,
};

const Card = ({ name, status, runNumber, href }: CardProps) => {
  return (
    <article className="flex flex-col items-start border-gray-200 border p-4 aspect-square gap-2 overflow-hidden">
        <div className="flex items-center gap-x-4 text-xs self-end">
          <a href="#" className="relative z-10 rounded-full bg-gray-50 px-3 py-1.5 font-medium text-gray-600 hover:bg-gray-100">{runNumber}</a>
        </div>
        <div className='w-full'>
          <h3 className="mt-3 text-lg/6 font-semibold text-gray-900 group-hover:text-gray-600 overflow-hidden text-ellipsis">
              <a href={href}>
              {name}
              </a>
          </h3>
        </div>
    </article>
  )
}

type Job = {
  name: string,
  status: string,
  runNumber: number,
  href: string,
}

function formatJobs(jobs: any): Array<Job> {
  return jobs.map((job: any) => {
    return {
      name: job.name,
      status: job.status,
      runNumber: job.run_number,
      href: `/job/${job.name}`,
    }
  })
}

function Dashboard() {
  const queryClient = useQueryClient();
  
  const { isPending, error, data, isFetching } = useQuery({
    queryKey: ['getJobs'],
    queryFn: async () => {
      const response = await fetch(
        `http://tron-infrastage.yelpcorp.com:8089/api/jobs?page=1&page_size=10`,
      )
      return await response.json()
    },
  })

  if (isPending) {
    return <div>Loading...</div>
  }


  if (!data || error) {
    console.log("Failed to fetch job details: " + error?.message)
  }

  const jobs = formatJobs(data.jobs);

  return (
    <div className="mx-auto max-w-7xl px-6 pb-16">
      <div className="mx-auto grid max-w-2xl grid-cols-1 gap-x-8 gap-y-16 border-t border-gray-200 pt-10 lg:mx-0 lg:max-w-none lg:grid-cols-6">
        {
          jobs.map(({ name, status, runNumber, href }) => (
            <Card name={name} status={status} runNumber={runNumber} href={href} />
          ))
        }
      </div>
    </div>
  );
}

export default Dashboard;
