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


class summary:
    def __init__(self):
        timezone = pytz.timezone('America/Mexico_City')
        today = date.today()
        yesterday = today - timedelta(days=1)
        date_ini = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, 0, timezone)
        date_end = datetime.datetime(today.year, today.month, today.day, 0, 0, 0, 0, timezone)

        branches = Branch.objects(status='active')
        for branch in branches:
            print(str(branch.name))
            if SummaryBranches.objects(branch_id=str(branch.id), date=date_ini).count() == 0:
                SummaryBranches(
                    branch_id=str(branch.id),
                    network_id=branch.network_id,
                    date=date_ini,
                ).save()

            summary = SummaryBranches.objects(branch_id=str(branch.id), date=date_ini).first()

            accumulated_connections = CampaignLog.objects(device__branch_id=str(branch.id),
                                                          interaction__accessed__exists=True,
                                                          created_at__lte=date_end).count()

            accumulated_devices = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$lte': date_end},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            accumulated_users = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$lte': date_end},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            accumulated_os = CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$lte': date_end},
                    }
                },
                {
                    '$group': {
                        '_id': '$device.os',
                        'devices': {
                            '$addToSet': '$device.mac'
                        }
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1},
                    }
                }
            ])
            accumulated_demographic = CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$lte': date_end},
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'gender': '$user.gender',
                            'age': '$user.age',
                        },
                        'users': {
                            '$addToSet': '$user.id'
                        }
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1},
                    }
                }
            ])

            accu_os = {}
            for os in accumulated_os:
                accu_os[str(os['_id'])] = os['count']

            accu_demographic = {
                'male': {},
                'female': {},
                'None': {}
            }
            for gender in accumulated_demographic:
                accu_demographic[str(gender['_id']['gender'])][str(gender['_id']['age'])] = gender['count']

            summary.accumulated = {
                'devices': {
                    'total': accumulated_devices[0]['count'] if len(accumulated_devices) > 0 else 0,
                    'os': accu_os,
                },
                'users': {
                    'total': accumulated_users[0]['count'] if len(accumulated_users) > 0 else 0,
                    'demographic': accu_demographic,
                },
                'connections': accumulated_connections,
            }

            print('|--- Accumulated [OK]')

            # Devices
            network_os = CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end}
                    }
                },
                {
                    '$group': {
                        '_id': '$device.os',
                        'devices': {
                            '$addToSet': '$device.mac'
                        }
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1},
                    }
                }
            ])

            network_welcome = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.welcome': {'$exists': True},
                        'interaction.welcome_loaded': {'$exists': False},
                        'interaction.joined': {'$exists': False},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_welcome_loaded = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.welcome_loaded': {'$exists': True},
                        'interaction.joined': {'$exists': False},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_joined = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.joined': {'$exists': True},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_requested = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.requested': {'$exists': True},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_loaded = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.loaded': {'$exists': True},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_completed = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.completed': {'$exists': True},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            network_accessed = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.accessed': {'$exists': True},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'devices': {'$addToSet': '$device.mac'}
                    }
                },
                {'$unwind': '$devices'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))

            devices_total = 0
            devices_os = {}
            for net in network_os:
                devices_os[str(net['_id'])] = net['count']
                devices_total += net['count']

            summary.devices = {
                'os': devices_os,
                'total': devices_total,
                'interactions': {
                    'welcome': network_welcome[0]['count'] if len(network_welcome) > 0 else 0,
                    'welcome_loaded': network_welcome_loaded[0]['count'] if len(network_welcome_loaded) > 0 else 0,
                    'joined': network_joined[0]['count'] if len(network_joined) > 0 else 0,
                    'requested': network_requested[0]['count'] if len(network_requested) > 0 else 0,
                    'loaded': network_loaded[0]['count'] if len(network_loaded) > 0 else 0,
                    'completed': network_completed[0]['count'] if len(network_completed) > 0 else 0,
                    'accessed': network_accessed[0]['count'] if len(network_accessed) > 0 else 0
                },
            }

            print('|--- Devices [OK]')

            # Users

            user_gender = CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'gender': '$user.gender',
                            'age': '$user.age',
                        },
                        'users': {
                            '$addToSet': '$user.id'
                        }
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1},
                    }
                }
            ])

            user_welcome = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.welcome': {'$exists': True},
                        'interaction.welcome_loaded': {'$exists': False},
                        'interaction.joined': {'$exists': False},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_welcome_loaded = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.welcome_loaded': {'$exists': True},
                        'interaction.joined': {'$exists': False},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_joined = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.joined': {'$exists': True},
                        'interaction.requested': {'$exists': False},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_requested = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.requested': {'$exists': True},
                        'interaction.loaded': {'$exists': False},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_loaded = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.loaded': {'$exists': True},
                        'interaction.completed': {'$exists': False},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_completed = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.completed': {'$exists': True},
                        'interaction.accessed': {'$exists': False},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            user_accessed = list(CampaignLog.objects().aggregate(*[
                {
                    '$match': {
                        'device.branch_id': str(branch.id),
                        'created_at': {'$gte': date_ini, '$lte': date_end},
                        'interaction.accessed': {'$exists': True},
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$addToSet': '$user.id'}
                    }
                },
                {'$unwind': '$users'},
                {
                    '$group': {
                        '_id': '$_id',
                        'count': {'$sum': 1}
                    }
                }
            ]))

            users_total = 0
            users_demographic = {
                'male': {},
                'female': {},
                'None': {}
            }
            for gender in user_gender:
                users_demographic[str(gender['_id']['gender'])][str(gender['_id']['age'])] = gender['count']
                users_total += gender['count']

            summary.users = {
                'total': users_total,
                'demographic': users_demographic,
                'interactions': {
                    'welcome': user_welcome[0]['count'] if len(user_welcome) > 0 else 0,
                    'welcome_loaded': user_welcome_loaded[0]['count'] if len(user_welcome_loaded) > 0 else 0,
                    'joined': user_joined[0]['count'] if len(user_joined) > 0 else 0,
                    'requested': user_requested[0]['count'] if len(user_requested) > 0 else 0,
                    'loaded': user_loaded[0]['count'] if len(user_loaded) > 0 else 0,
                    'completed': user_completed[0]['count'] if len(user_completed) > 0 else 0,
                    'accessed': user_accessed[0]['count'] if len(user_accessed) > 0 else 0
                },
            }

            print('|--- Users [OK]')

            # Summary
            summary.save()
