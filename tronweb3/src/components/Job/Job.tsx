import './../../App.css'
import {
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import jobdata from './JobData.json';
import JobDetails from './JobDetails';
import ActionGraph from './ActionGraph';
import { useParams } from '@tanstack/react-router';


function Job() {
  const { jobId } = useParams({ strict: false});
  const queryClient = useQueryClient()
  const { isPending, error, data, isFetching } = useQuery({
    queryKey: ['getJobs'],
    queryFn: async () => {
      const response = await fetch(
        `http://tron-infrastage.yelpcorp.com:8089/api/jobs/${jobId}?include_action_graph=1`,
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
  
  return (
    <>
    <div className="mx-auto max-w-7xl px-6 pb-16">
      <div className="container mx-auto m-10 p-10">
        <h1 className="text-lg font-semibold">{data.name}</h1>
      </div>
      <div className="grid grid-cols-2 gap-4">
        <JobDetails 
          status={data.status}
          node_pool={data.node_pool}
          scheduler={data.scheduler}
          last_success={data.last_success}
        />
        <ActionGraph graphData={data.action_graph} />
      </div>
      </div>
    </>
  );
}

export default Job;
