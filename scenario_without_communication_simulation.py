import asyncio
from datetime import datetime, timedelta

import mango.messages.codecs
import numpy as np
import pandas as pd
import time
from mango import create_container

import mango_library.negotiation.util as util
from agents.aggregator_agent import AggregatorAgent
from agents.grid_agent import GridAgent
from agents.messages import RedispatchFlexibilityRequest, RedispatchFlexibilityReply, AggregatedSolutionMessage, \
    CallForAdaption
from agents.unit_agents import WindAgent, PVAgent, CHPAgent, BatteryAgent, LoadAgent
from config import NUMBER_OF_WIND_AGENTS, ATTACK_SCENARIO, MANIPULATED_AGENT_ID, NUMBER_OF_CHPS, \
    NUMBER_OF_AGENTS_TOTAL, NUMBER_OF_PV_AGENTS, NUMBER_OF_BATTERIES, INITIAL_TARGET_PARAMS, NUMBER_OF_HOUSEHOLDS, \
    NEGOTIATION_TIMEOUT
from mango_library.coalition.core import (
    CoalitionParticipantRole,
    CoalitionInitiatorRole,
)
from mango_library.negotiation.cohda.cohda_negotiation import (
    COHDANegotiationRole,
)
from mango_library.negotiation.cohda.cohda_solution_aggregation import (
    CohdaSolutionAggregationRole,
)
from mango_library.negotiation.cohda.cohda_starting import CohdaNegotiationInteractiveStarterRole
from mango_library.negotiation.termination import (
    NegotiationTerminationParticipantRole,
    NegotiationTerminationDetectorRole,
)
from pysimmods.util.date_util import GER


def maximize_self_consumption(cs, target_params):
    """
    Target to maximize the self-consumption. Target is therefore 0, penalty is applied if renewable energies
    are reduced. When obligations are received from operator agent, target is adapted accordingly.
    """
    penalty = target_params['penalty']
    sum_cs = cs.sum(axis=0)  # sum for each interval
    target = [0. for _ in range(len(sum_cs))]
    start = target_params['current_start_date']
    if 'obligations' in target_params.keys():
        # for each obligation, update value in target list at that position
        for entry in target_params['obligations']:
            obligation_date = entry[0]
            obligation_value = entry[1]
            obligation_idx = 0
            obligation_date_found = False
            obligation_date_obj = datetime.strptime(obligation_date + 'Z', GER)
            start_obj = datetime.strptime(start + 'Z', GER)
            if obligation_date_obj < start_obj:
                continue
            while not obligation_date_found:
                if start == obligation_date:
                    target[obligation_idx] = obligation_value
                    obligation_date_found = True
                else:
                    obligation_idx += 1
                    if obligation_idx >= len(target):
                        obligation_date_found = True
                    else:
                        # shift date to next interval, 15 minutes later
                        start = (datetime.strptime(start + 'Z', GER) + timedelta(minutes=15)).strftime(GER)
                        start = start[0:len(start) - 5]
    diff = np.abs(np.array(target) - sum_cs)  # deviation to the target schedule
    result = -np.sum(diff) - penalty
   # total_max_target_deviation = np.sum(target_params["max_target_deviation"])
    #norm_result = (result - 0) / (total_max_target_deviation * (1-1/target_params['n_agents']) - 0)
    return float(result)



async def redispatch_scenario():
    pd.DataFrame({'manipulated': MANIPULATED_AGENT_ID}, index=[0]).to_csv(f'manipulated_agent.csv', index=False)

    # create containers
    codec = mango.messages.codecs.JSON()
    for serializer in util.cohda_serializers:
        codec.add_serializer(*serializer())
    codec.add_serializer(*RedispatchFlexibilityRequest.__serializer__())
    codec.add_serializer(*RedispatchFlexibilityReply.__serializer__())
    codec.add_serializer(*AggregatedSolutionMessage.__serializer__())
    codec.add_serializer(*CallForAdaption.__serializer__())
    containers = []
    cohda_agents = []
    addrs = []
    agent_names = []

    client_container_mapping = {}
    household_ids = 0
    batt_ids = 0
    generation_ids = 0

    current_container = await create_container(addr=("127.0.0.2", 5555), codec=codec,
                                               # connection_type='tcp',
                                               manipulation_id=MANIPULATED_AGENT_ID, attack_scenario=ATTACK_SCENARIO)
    # controller agent
    aggregator_agent = AggregatorAgent(current_container, n_agents=NUMBER_OF_AGENTS_TOTAL,
                                       suggested_aid=f'aggregator_agent', target_params=INITIAL_TARGET_PARAMS,
                                       negotiation_timeout=NEGOTIATION_TIMEOUT)
    aggregator_agent.add_role(NegotiationTerminationDetectorRole())
    agent_names.append(aggregator_agent.aid)
    client_container_mapping[f'aggregator_agent'] = current_container

    # controller agent
    grid_agent = GridAgent(current_container, suggested_aid=f'grid_operator_agent')
    agent_names.append(grid_agent.aid)
    client_container_mapping[f'grid_operator_agent'] = current_container

    aggregator_agent.grid_agent_addr = (current_container.addr, grid_agent.aid)

    for w_i in range(NUMBER_OF_WIND_AGENTS):
        a = WindAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}')
        addrs.append((current_container.addr, a.aid))
        cohda_role = COHDANegotiationRole(schedules_provider=a.schedule_provider,
                                          perf_func=maximize_self_consumption,
                                          attack_scenario=ATTACK_SCENARIO,
                                          manipulated_agent=MANIPULATED_AGENT_ID,
                                          store_updates_to_db=True,
                                          penalty=a.calculate_penalty,
                                          container=current_container,
                                          agent=f'generation_agent_{generation_ids}')
        a.add_role(cohda_role)
        a.add_role(CoalitionParticipantRole())
        a.add_role(NegotiationTerminationParticipantRole())
        if w_i == 0:
            a.add_role(CohdaNegotiationInteractiveStarterRole(target_params=INITIAL_TARGET_PARAMS,
                                                              container=current_container))
            aggregation_role = CohdaSolutionAggregationRole()
            aggregator_agent.add_role(aggregation_role)
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'generation_agent_{generation_ids}'] = current_container
        generation_ids += 1

    for _ in range(NUMBER_OF_PV_AGENTS):
        a = PVAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}')
        addrs.append((current_container.addr, a.aid))
        cohda_role = COHDANegotiationRole(schedules_provider=a.schedule_provider,
                                          perf_func=maximize_self_consumption,
                                          attack_scenario=ATTACK_SCENARIO,
                                          manipulated_agent=MANIPULATED_AGENT_ID,
                                          store_updates_to_db=True,
                                          penalty=a.calculate_penalty,
                                          container=current_container,
                                          agent=f'generation_agent_{generation_ids}')
        a.add_role(cohda_role)
        a.add_role(CoalitionParticipantRole())
        a.add_role(NegotiationTerminationParticipantRole())
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'generation_agent_{generation_ids}'] = current_container
        generation_ids += 1

    for _ in range(NUMBER_OF_CHPS):
        a = CHPAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}')
        addrs.append((current_container.addr, a.aid))
        cohda_role = COHDANegotiationRole(schedules_provider=a.schedule_provider,
                                          perf_func=maximize_self_consumption,
                                          attack_scenario=ATTACK_SCENARIO,
                                          manipulated_agent=MANIPULATED_AGENT_ID,
                                          store_updates_to_db=True,
                                          penalty=a.calculate_penalty,
                                          container=current_container,
                                          agent=f'generation_agent_{generation_ids}')
        a.add_role(cohda_role)
        a.add_role(CoalitionParticipantRole())
        a.add_role(NegotiationTerminationParticipantRole())
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'generation_agent_{generation_ids}'] = current_container
        generation_ids += 1

    for _ in range(NUMBER_OF_BATTERIES):
        a = BatteryAgent(current_container, suggested_aid=f'storage_agent_{batt_ids}')
        addrs.append((current_container.addr, a.aid))
        cohda_role = COHDANegotiationRole(schedules_provider=a.schedule_provider,
                                          perf_func=maximize_self_consumption,
                                          attack_scenario=ATTACK_SCENARIO,
                                          manipulated_agent=MANIPULATED_AGENT_ID,
                                          store_updates_to_db=True,
                                          penalty=a.calculate_penalty,
                                          container=current_container,
                                          agent=f'storage_agent_{batt_ids}')
        a.add_role(cohda_role)
        a.add_role(CoalitionParticipantRole())
        a.add_role(NegotiationTerminationParticipantRole())
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'storage_agent_{batt_ids}'] = current_container
        batt_ids += 1

    for _ in range(NUMBER_OF_HOUSEHOLDS):
        a = LoadAgent(current_container, suggested_aid=f'household_agent_{household_ids}')
        addrs.append((current_container.addr, a.aid))
        cohda_role = COHDANegotiationRole(schedules_provider=a.schedule_provider,
                                          perf_func=maximize_self_consumption,
                                          attack_scenario=ATTACK_SCENARIO,
                                          manipulated_agent=MANIPULATED_AGENT_ID,
                                          store_updates_to_db=True,
                                          penalty=a.calculate_penalty,
                                          container=current_container,
                                          agent=f'household_agent_{household_ids}')
        a.add_role(cohda_role)
        a.add_role(CoalitionParticipantRole())
        a.add_role(NegotiationTerminationParticipantRole())
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'household_agent_{household_ids}'] = current_container
        household_ids += 1
    print('start now', agent_names)
    aggregator_agent.agent_names = agent_names
    aggregator_agent.agent_addrs = addrs
    aggregator_agent.cohda_agents = [a.aid for a in cohda_agents]
    start = time.time()
    print('give coalition initiator role', start)
    aggregator_agent.add_role(
        CoalitionInitiatorRole(addrs, "cohda", "cohda-negotiation")
    )

    for a in cohda_agents + [aggregator_agent]:
        if a._check_inbox_task.done():
            if a._check_inbox_task.exception() is not None:
                raise a._check_inbox_task.exception()
            else:
                assert False, f"check_inbox terminated unexpectedly."

    await aggregator_agent.step_done
    print('Done', time.time() - start)
    time.sleep(2)
    print('shutdown')
    for container in containers:
        await container.shutdown()
    print('close connection now.')


def main():
    asyncio.run(redispatch_scenario())


if __name__ == '__main__':
    main()
