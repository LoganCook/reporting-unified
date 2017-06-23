from . import app, configure, request, instance_method
from . import get_or_create, commit
from . import Resource, QueryResource, BaseIngestResource, RangeQuery

from ..models.slurm import Job


class PartitionResource(Resource):
    """HPC Slurm partition"""
    def get(self):
        return Job.list_partition()


class UserResource(QueryResource):
    """HPC Job User"""
    def get(self):
        return Job.list_user()


class JobResource(QueryResource):
    """HPC Job"""
    query_class = Job


class JobList(RangeQuery):
    def _get(self, **kwargs):
        return Job.list(start_ts=kwargs['start'], end_ts=kwargs['end'])


class JobSummary(RangeQuery):
    def _get(self, **kwargs):
        return Job.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest jobs."""

        messages = [message for message in request.get_json(force=True)
                    if message["schema"] == "hpc.slurm"]

        for message in messages:
            for job in message["data"]["jobs"]:
                get_or_create(Job, **job)

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/partition": PartitionResource,
        "/owner": UserResource,
        "/job": JobResource,
        "/job/list": JobList,
        "/job/summary": JobSummary,
        "/ingest": IngestResource
    }

    configure(resources)


setup()
