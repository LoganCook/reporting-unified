from utils import parse_date_string

from . import app, configure, request, instance_method
from . import get_or_create, commit
from . import Resource, QueryResource, BaseIngestResource, RangeQuery

from ..models.vms import Instance


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest instances."""

        messages = [message for message in request.get_json(force=True)
                    if message["schema"] == "cloud.tango"]

        for message in messages:
            for instance in message["data"]["instances"]:
                # need to convert month from a sting like 2017-12-01 to timestamp
                # to be consistant with other usage data
                instance['month'] = parse_date_string(instance['month'], fmt='%Y-%m-%d')
                get_or_create(Instance, **instance)

        commit()

        return "", 204


class InstanceResource(RangeQuery):
    """Instance Endpoint"""
    def _get(self, **kwargs):
        return Instance.list(start_ts=kwargs['start'], end_ts=kwargs['end'])


def setup():
    resources = {
        "/instance": InstanceResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()
