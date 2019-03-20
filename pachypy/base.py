from typing import Optional

import pandas as pd

from python_pachyderm import PpsClient, PfsClient
from python_pachyderm.client.pps.pps_pb2 import ListJobRequest, CreatePipelineRequest, DeletePipelineRequest, Pipeline
from python_pachyderm.pps_client import JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED
from python_pachyderm.pps_client import PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY


class PythonPachydermWrapper:

    """Wrapper around client objects of the python_pachyderm package for easier interaction.

    This is the basis for the PachydermClient class and is not intended to be used directly.

    Args:
        host: Hostname or IP address to reach pachd.
        post: Port on which pachd is listening.
    """

    def __init__(self, host: str = 'localhost', port: int = 30650):
        self.pps_client = PpsClient(host, port)
        self.pfs_client = PfsClient(host, port)

    def _list_repo(self) -> pd.DataFrame:
        """Returns list of repositories."""
        res = []
        for repo in self.pfs_client.list_repo():
            res.append({
                'repo': repo.repo.name,
                'size_bytes': repo.size_bytes,
                'branches': len(repo.branches),
                'created': repo.created.seconds,
            })
        return pd.DataFrame(res)

    def _list_pipeline(self) -> pd.DataFrame:
        """Returns list of pipelines."""
        state_mapping = {
            PIPELINE_STARTING: 'starting',
            PIPELINE_RUNNING: 'running',
            PIPELINE_RESTARTING: 'restarting',
            PIPELINE_FAILURE: 'failure',
            PIPELINE_PAUSED: 'paused',
            PIPELINE_STANDBY: 'standby',
        }
        res = []
        for pipeline in self.pps_client.list_pipeline().pipeline_info:
            res.append({
                'pipeline': pipeline.pipeline.name,
                'parallelism': pipeline.parallelism_spec.constant,
                'created': float(f'{pipeline.created_at.seconds}.{pipeline.created_at.nanos}'),
                'state': state_mapping.get(pipeline.state, 'unknown'),
            })
        return pd.DataFrame(res)

    def _list_job(self, pipeline: Optional[str] = None, n: int = 20) -> pd.DataFrame:
        """Returns list of last n jobs.

        Args:
            pipeline: Name of pipeline to return jobs for. Returns all jobs if not specified.
            n: Maximum number of jobs to return.
        """
        state_mapping = {
            JOB_STARTING: 'starting',
            JOB_RUNNING: 'running',
            JOB_FAILURE: 'failure',
            JOB_SUCCESS: 'success',
            JOB_KILLED: 'killed',
        }
        i = 1
        res = []
        for job in self.pps_client.stub.ListJobStream(ListJobRequest(pipeline=Pipeline(name=pipeline))):
            res.append({
                'job': job.job.id,
                'pipeline': job.pipeline.name,
                'state': state_mapping.get(job.state, 'unknown'),
                'started': float(f'{job.started.seconds}.{job.started.nanos}'),
                'finished': float(f'{job.finished.seconds}.{job.finished.nanos}'),
                'restart': job.restart,
                'data_processed': job.data_processed,
                'data_skipped': job.data_skipped,
                'data_total': job.data_total,
                'download_time': float(f'{job.stats.download_time.seconds}.{job.stats.download_time.nanos}'),
                'process_time': float(f'{job.stats.process_time.seconds}.{job.stats.process_time.nanos}'),
                'upload_time': float(f'{job.stats.upload_time.seconds}.{job.stats.upload_time.nanos}'),
                'download_bytes': job.stats.download_bytes,
                'upload_bytes': job.stats.upload_bytes
            })
            i += 1
            if n is not None and i > n:
                break
        return pd.DataFrame(res)

    def _get_logs(self, pipeline: Optional[str] = None, job: Optional[str] = None, master: bool = False) -> pd.DataFrame:
        """Returns log entries.

        Args:
            pipeline: Name of pipeline to filter logs by.
            job: ID of job to filter logs by. (optional)
            master: Whether to return logs from the Pachyderm master process.
        """
        res = []
        for msg in self.pps_client.get_logs(pipeline_name=pipeline, job_id=job, master=master):
            res.append({
                'pipeline': msg.pipeline_name,
                'job': msg.job_id,
                'ts': float(f'{msg.ts.seconds}.{msg.ts.nanos}'),
                'message': msg.message,
                'worker': msg.worker_id,
                'datum': msg.datum_id,
                'user': msg.user,
            })
        return pd.DataFrame(res)

    def _create_pipeline(self, pipeline_specs: dict) -> None:
        """Create pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))

    def _update_pipeline(self, pipeline_specs: dict, reprocess: bool = False) -> None:
        """Update existing pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
            reprocess: Whether to reprocess datums with updated pipeline.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))

    def _delete_pipeline(self, pipeline: str) -> None:
        """Delete pipeline.

        Args:
            pipeline: Name of pipeline to delete.
        """
        self.pps_client.stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))
