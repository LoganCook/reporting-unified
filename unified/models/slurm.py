from sqlalchemy import UniqueConstraint, distinct
from sqlalchemy.sql import func
from . import db


class Job(db.Model):
    """Jobs reported by Slurm sacct command"""
    id = db.Column(db.Integer, primary_key=True)
    # once job_id reaches MaxJobId (default=2**26-1=67108863)
    # it starts from FirstJobId. From slurm.conf.html
    job_id = db.Column(db.Integer, nullable=False)
    user = db.Column(db.String(64), nullable=False)
    partition = db.Column(db.String(64), nullable=False)
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)
    cpu_seconds = db.Column(db.Integer)
    __table_args__ = (UniqueConstraint('job_id', 'start', name='uix_job'),)

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "owner": self.user,
            "partition": self.partition,
            "start": self.start,
            "end": self.end,
            "cpu_seconds": self.cpu_seconds
        }

    @classmethod
    def list(cls, start_ts=0, end_ts=0):
        """"Gets jobs finished between start_ts and end_ts.
        """
        query = cls.query

        if start_ts > 0:
            query = query.filter(Job.end >= start_ts)
        if end_ts > 0:
            query = query.filter(Job.end < end_ts)
        fields = ['job_id', 'owner', 'partition', 'start', 'end', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query.
                with_entities(Job.job_id,
                              Job.user,
                              Job.partition,
                              Job.start,
                              Job.end)]

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets job statistics of which finished between start_ts and end_ts.

        Grouped by owner then partition
        """
        query = cls.query
        if start_ts > 0:
            query = query.filter(Job.end >= start_ts)
        if end_ts > 0:
            query = query.filter(Job.end < end_ts)

        query = query.group_by(Job.user, Job.partition).\
            with_entities(Job.user, Job.partition,
                          func.count(Job.job_id),
                          func.sum(Job.cpu_seconds))

        fields = ['owner', 'partition', 'job_count', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query]

    @classmethod
    def list_user(cls):
        """List unique user names"""
        return [item[0] for item in cls.query.distinct(Job.user).
                with_entities(Job.user)]

    @classmethod
    def list_partition(cls):
        """List unique partitions"""
        return [item[0] for item in cls.query.distinct(Job.partition).
                with_entities(Job.partition)]
