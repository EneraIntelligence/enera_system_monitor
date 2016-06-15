from mongoengine import *
import datetime
import pytz

connect('enera', username='enera', password='enera', host='ds056998.mlab.com', port=56998)
connect('enera', username='', password='', host='query0.enera-intelligence.mx')


class SummaryCmx(DynamicDocument):
    created_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    updated_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    meta = {'collection': 'sumary_cmx'}

    def save(self, *args, **kwargs):
        # if not self.created_at:
        #     self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now(pytz.utc)
        return super(SummaryCmx, self).save(*args, **kwargs)


class CmxRaw(DynamicDocument):
    meta = {'collection': 'cmx_raw'}


class Devices(DynamicDocument):
    meta = {'collection': 'devices'}


class Network(DynamicDocument):
    meta = {'collection': 'networks'}


class SummaryNetwork(DynamicDocument):
    created_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    updated_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    meta = {'collection': 'summary_networks'}

    def save(self, *args, **kwargs):
        # if not self.created_at:
        #     self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now(pytz.utc)
        return super(SummaryNetwork, self).save(*args, **kwargs)


class SummaryBranches(DynamicDocument):
    created_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    updated_at = DateTimeField(default=datetime.datetime.now(pytz.utc))
    meta = {'collection': 'summary_branches'}

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(pytz.utc)
        return super(SummaryBranches, self).save(*args, **kwargs)


class Branch(DynamicDocument):
    meta = {'collection': 'branches'}


class Administrator(DynamicDocument):
    meta = {'collection': 'administrators'}


class User(DynamicDocument):
    meta = {'collection': 'users'}


class CampaignLog(DynamicDocument):
    meta = {'collection': 'campaign_logs'}
