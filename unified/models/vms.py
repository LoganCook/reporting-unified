from sqlalchemy import PrimaryKeyConstraint
from . import db


class Instance(db.Model):
    """Monthly report of virtual machine instances produced by VRB vms report"""
    server_id = db.Column(db.String(), nullable=False)
    server = db.Column(db.String(), nullable=False)
    core = db.Column(db.SmallInteger, nullable=False)
    ram = db.Column(db.SmallInteger, nullable=False)
    storage = db.Column(db.Float)
    os = db.Column(db.String(), nullable=False)
    business_unit = db.Column(db.String(), nullable=False)
    span = db.Column(db.Integer)
    # Monthly Up Time (%)
    uptime_percent = db.Column(db.Float)
    # timestamp of the start of reporting month
    month = db.Column(db.Integer)
    __table_args__ = (PrimaryKeyConstraint('server_id', 'month', name='pk_instance'),)

    def json(self):
        """Jsonify"""
        return {
            "id": self.server_id,
            "server": self.server,
            "core": self.core,
            "ram": self.ram,
            "storage": self.storage,
            "os": self.os,
            "businessUnit": self.business_unit,
            "span": self.span,
            "uptimePercent": self.uptime_percent,
            "month": self.month
        }

    @classmethod
    def list(cls, start_ts=0, end_ts=0):
        """"Gets vms run between start_ts and end_ts."""
        query = cls.query

        if start_ts > 0:
            query = query.filter(Instance.month >= start_ts)
        if end_ts > 0:
            query = query.filter(Instance.month < end_ts)
        fields = ('id', 'server', 'core', 'ram', 'storage', 'os', 'businessUnit', 'span', 'uptimePercent')
        return [dict(zip(fields, q)) for q in query.
                with_entities(Instance.server_id,
                              Instance.server,
                              Instance.core,
                              Instance.ram,
                              Instance.storage,
                              Instance.os,
                              Instance.business_unit,
                              Instance.span,
                              Instance.uptime_percent)]
