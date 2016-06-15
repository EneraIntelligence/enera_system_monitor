import json
from models import *
from bson import ObjectId
import pprint

import pytz
import datetime
from datetime import date, timedelta


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
            return json.JSONEncoder.default(self, o)


class Tracking:
    def __init__(self):
        print('devices')
        timezone = pytz.timezone('America/Mexico_City')
        today = date.today()
        yesterday = today - timedelta(days=1)
        date_ini = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, 0, timezone)
        date_end = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 0, 0, timezone)
        # date_end = datetime.datetime(today.year, today.month, today.day, 0, 0, 0, 0, timezone)
        print(date_end)
        accumulated_devices = list(CmxRaw.objects().aggregate(*[
            {
                '$match': {
                    'device.last_seen': {'$gte': date_ini, '$lte': date_end},
                    # 'device.last_seen': {'$lte': '2016-06-14T23:00:00.000-0600'},
                    'ap.branch_id': ObjectId("57225110cf44ed034ec3cbab")
                }
            },
            {
                '$sort': {
                    'device.mac': 1,
                    'device.last_seen': -1
                }
            },
            # {'$unwind': '$devices'},
            {
                '$group': {
                    '_id': {
                        'branch': '$ap.branch_id',
                        'ap_mac': '$ap.mac',
                        'd_mac': '$device.mac'
                    },
                    'last_seen': {
                        '$addToSet': '$device.last_seen'
                    }
                }
            }
        ]))
        accu_device = {}
        try:
            for os in accumulated_devices:
                accu_device[str(os['_id'])] = os['count']
            print('si funciono')
            print(accu_os)
        except Exception as e:
            print('error en el for')

            # branches = Branch.objects(status='active')
            # for b in branches:
            #     print(b.aps)
            # devices = CmxRaw.objects(device__last_seen__lte=date_end, device__last_seen__gte=date_ini,
            #                          ap__branch_id=b.id)


class Device:
    def __init__(self):
        print('devices')
        timezone = pytz.timezone('America/Mexico_City')
        today = date.today()
        yesterday = today - timedelta(days=1)
        date_ini = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, 0, timezone)
        date_end = datetime.datetime(today.year, today.month, today.day, 0, 0, 0, 0, timezone)
        print(date_end)
        # devices = CmxRaw.objects(device_last_seen__gte='2016-06-13T11:21:59.000-0600')
        branches = Branch.objects(status='active')
        for b in branches:
            devices = CmxRaw.objects(device__last_seen__lte=date_end, device__last_seen__gte=date_ini,
                                     ap__branch_id=b.id).count()
            print(str(devices))
            print(str(b.id))
            # for d in devices:
            #     print(str(len(b)))
            #     print(d.ap['mac'])
            # branches_id.append(json.loads(JSONEncoder().encode(branch)))
            # print(str(len(devices)))

            # puntos = CmxRaw.objects(id=ObjectId(com)).first()
            # data = data_set.objects(
            #     location__geo_within_sphere=[[longitude, latitude], radius / 6371.0]
            # )
            # class Geo(Document):
            #     loc = PointField()
            #
            # result = Geo.objects(loc__near=[3, 6], loc__max_distance=1000)
