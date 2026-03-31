from datetime import datetime
from enum import Enum


from mango_library.negotiation.cohda.cohda_starting import CohdaNegotiationInteractiveStarterRole


def datetime_to_index(
        dt: datetime, step_size: int = 900, steps_per_year: int = 35135
) -> int:
    dif = dt - dt.replace(month=1, day=1, hour=0, minute=0, second=0)
    dif = dif.total_seconds()
    idx = int(dif // step_size) % steps_per_year

    return idx



class AgentType(Enum):
    AGGREGATOR = 0
    OPERATOR = 1
    WIND = 2
    PV = 3
    CHP = 4
    BATTERY = 5
    HOUSEHOLD = 6


# 0: No Failure
# 5: Asset failure
# 6: Communication failure
# 7. Communication delays
# 8. Compromised process data
# 9. No communication from Aggregator Agent to neighborhood grid
# 10. No communication from neighborhood grid to Aggregator agent
class ATTACK_TYPE(Enum):
    NONE = 0
    ASSET_FAILURE = 5
    COMMUNICATION_FAILURE = 6
    DELAYS = 7
    COMPROMISED_DATA = 8
    COMMUNICATION_TO_FIELD = 9
    COMMUNICATION_FROM_FIELD = 10


class INCIDENT(Enum):
    FAILURE = 0
    DATA = 1
    DELAYS = 2
