import asyncio
import time
from datetime import datetime, timedelta

import mango.messages.codecs
import numpy as np
import pandas as pd
from mango import create_container
from pysimmods.util.date_util import GER

import mango_library.negotiation.util as util
from agents.aggregation_role import AggregationRole
from agents.aggregator_agent import AggregatorAgent
from agents.centralized_observer import CentralizedObserverAgentRole
from agents.controller_agent import CentralizedControllerAgent
from agents.decentralized_controller import DecentralizedController
from agents.decentralized_observer import DecentralizedObserverRole
from agents.messages import RedispatchFlexibilityRequest, RedispatchFlexibilityReply, AggregatedSolutionMessage, \
    CallForAdaption, CoalitionAdaption, CallForExclusion, ReassignRole, CallForNewTopology, Inactive
from agents.operator_agent import OperatorAgent
from agents.unit_agents import WindAgent, PVAgent, CHPAgent, BatteryAgent, LoadAgent
from config import NUMBER_OF_WIND_AGENTS, ATTACK_SCENARIO, MANIPULATED_AGENT_ID, NUMBER_OF_CHPS, \
    NUMBER_OF_AGENTS_TOTAL, NUMBER_OF_PV_AGENTS, NUMBER_OF_BATTERIES, INITIAL_TARGET_PARAMS, NUMBER_OF_HOUSEHOLDS, \
    NEGOTIATION_TIMEOUT, CENTRALIZED_CONTROL, DECENTRALIZED_CONTROL, MULTI_LEVELLED, ID_AGENT_MAPPING
from cosima_core.mango_direct_connection.mango_communication_network import MangoCommunicationNetwork
from cosima_core.util.general_config import PORT
from cosima_core.util.util_functions import stop_omnet
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
from util import AgentType, ATTACK_TYPE


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
    # norm_result = (result - 0) / (total_max_target_deviation * (1-1/target_params['n_agents']) - 0)
    return float(result)


async def redispatch_scenario():
    pd.DataFrame({'manipulated': MANIPULATED_AGENT_ID}, index=[0]).to_csv(
        f'agents/manipulated_agent.csv', index=False)
    observer = None
    # create containers
    codec = mango.messages.codecs.JSON()
    for serializer in util.cohda_serializers:
        codec.add_serializer(*serializer())
    codec.add_serializer(*RedispatchFlexibilityRequest.__serializer__())
    codec.add_serializer(*RedispatchFlexibilityReply.__serializer__())
    codec.add_serializer(*AggregatedSolutionMessage.__serializer__())
    codec.add_serializer(*CallForAdaption.__serializer__())
    codec.add_serializer(*CoalitionAdaption.__serializer__())
    codec.add_serializer(*CallForExclusion.__serializer__())
    codec.add_serializer(*ReassignRole.__serializer__())
    codec.add_serializer(*AggregationRole.__serializer__())
    codec.add_serializer(*CallForNewTopology.__serializer__())
    codec.add_serializer(*CohdaNegotiationInteractiveStarterRole.__serializer__())
    codec.add_serializer(*Inactive.__serializer__())

    containers = []
    cohda_agents = []
    addrs = []
    agent_names = []
    agent_type_mapping = {}

    client_container_mapping = {}
    household_ids = 0
    batt_ids = 0
    generation_ids = 0

    if CENTRALIZED_CONTROL or MULTI_LEVELLED:
        # controller agent
        current_container = await create_container(addr=f"controller_agent", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)

        containers.append(current_container)
        controller = CentralizedControllerAgent(current_container, suggested_aid='controller_agent',
                                                n_agents=NUMBER_OF_AGENTS_TOTAL)
        client_container_mapping['controller_agent'] = current_container
        agent_names.append(controller.aid)
        controller_addr = controller.addr
        controller_aid = controller.aid

        if not MULTI_LEVELLED:
            observer = CentralizedObserverAgentRole(controller)
            controller.add_role(observer)
            controller.observer_module = observer
    else:
        controller_addr = None
        controller_aid = None

    # aggregator agent
    current_container = await create_container(addr=f"aggregator_agent", codec=codec,
                                               connection_type='external_connection',
                                               manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                               attack_scenario=ATTACK_SCENARIO)
    aggr_cont = current_container
    aggregator_agent = AggregatorAgent(current_container,
                                       suggested_aid=f'aggregator_agent', controller=[controller_addr, controller_aid])
    if CENTRALIZED_CONTROL:
        current_container.ctrl = controller

    term_detector = NegotiationTerminationDetectorRole()
    aggregator_agent.add_role(term_detector)
    aggregation_role = AggregationRole(n_agents=NUMBER_OF_AGENTS_TOTAL, target_params=INITIAL_TARGET_PARAMS,
                                       negotiation_timeout=NEGOTIATION_TIMEOUT,
                                       controller=(controller_addr, controller_aid))
    aggregator_agent.add_role(aggregation_role)
    agent_names.append(aggregator_agent.aid)
    client_container_mapping[f'aggregator_agent'] = current_container
    agent_type_mapping[(aggregator_agent.addr, aggregator_agent.aid)] = AgentType.AGGREGATOR
    containers.append(current_container)
    if CENTRALIZED_CONTROL or MULTI_LEVELLED:
        controller.aggregation_role = aggregation_role

    # operator agent
    current_container = await create_container(addr=f"operator_agent", codec=codec,
                                               connection_type='external_connection',
                                               manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                               attack_scenario=ATTACK_SCENARIO)

    operator_agent = OperatorAgent(current_container, suggested_aid=f'operator_agent')
    agent_names.append(operator_agent.aid)
    client_container_mapping[f'operator_agent'] = current_container

    aggregation_role.operator_agent_addr = (current_container.addr, operator_agent.aid)
    agent_type_mapping[(operator_agent.addr, operator_agent.aid)] = AgentType.OPERATOR
    containers.append(current_container)

    for w_i in range(NUMBER_OF_WIND_AGENTS):
        current_container = await create_container(addr=f"generation_agent_{generation_ids}", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)
        if w_i == 0 and CENTRALIZED_CONTROL or (w_i == 0 and DECENTRALIZED_CONTROL):
            a = WindAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}', aggr_cont=aggr_cont,
                          obs=observer, aggr=aggregator_agent, controller_addr=controller_addr,
                          controller_aid=controller_aid)
        else:
            a = WindAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}', aggr=aggregator_agent,
                          controller_addr=controller_addr, controller_aid=controller_aid)

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
            sol_aggregation_role = CohdaSolutionAggregationRole()
            aggregator_agent.add_role(sol_aggregation_role)
        cohda_agents.append(a)

        containers.append(current_container)
        agent_names.append(a.aid)
        client_container_mapping[f'generation_agent_{generation_ids}'] = current_container
        generation_ids += 1
        agent_type_mapping[(a.addr, a.aid)] = AgentType.WIND
        if DECENTRALIZED_CONTROL:
            d_ctrl = DecentralizedController(aggregator=(aggregator_agent.addr, aggregator_agent.aid),
                                             container=current_container)
            if MULTI_LEVELLED:
                d_ctrl.central_controller = (controller.addr, controller.aid)
            a.add_role(d_ctrl)
            d_obs = DecentralizedObserverRole(controller=d_ctrl)
            a.observer_module = d_obs
            a.add_role(d_obs)

    for _ in range(NUMBER_OF_PV_AGENTS):
        current_container = await create_container(addr=f"generation_agent_{generation_ids}", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)
        a = PVAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}',
                    controller_addr=controller_addr, controller_aid=controller_aid)
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
        agent_type_mapping[(a.addr, a.aid)] = AgentType.PV
        if DECENTRALIZED_CONTROL:
            d_ctrl = DecentralizedController(aggregator=(aggregator_agent.addr, aggregator_agent.aid))
            if MULTI_LEVELLED:
                d_ctrl.central_controller = (controller.addr, controller.aid)
            a.add_role(d_ctrl)
            d_obs = DecentralizedObserverRole(controller=d_ctrl)
            a.observer_module = d_obs
            a.add_role(d_obs)
    for _ in range(NUMBER_OF_CHPS):
        current_container = await create_container(addr=f"generation_agent_{generation_ids}", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)
        a = CHPAgent(current_container, suggested_aid=f'generation_agent_{generation_ids}',
                     controller_addr=controller_addr, controller_aid=controller_aid)
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
        agent_type_mapping[(a.addr, a.aid)] = AgentType.CHP
        if DECENTRALIZED_CONTROL:
            d_ctrl = DecentralizedController(aggregator=(aggregator_agent.addr, aggregator_agent.aid))
            if MULTI_LEVELLED:
                d_ctrl.central_controller = (controller.addr, controller.aid)
            a.add_role(d_ctrl)
            d_obs = DecentralizedObserverRole(controller=d_ctrl)
            a.observer_module = d_obs
            a.add_role(d_obs)

    for _ in range(NUMBER_OF_BATTERIES):
        current_container = await create_container(addr=f"storage_agent_{batt_ids}", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)
        a = BatteryAgent(current_container, suggested_aid=f'storage_agent_{batt_ids}', controller_addr=controller_addr,
                         controller_aid=controller_aid)
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
        agent_type_mapping[(a.addr, a.aid)] = AgentType.BATTERY
        if DECENTRALIZED_CONTROL:
            d_ctrl = DecentralizedController(aggregator=(aggregator_agent.addr, aggregator_agent.aid))
            if MULTI_LEVELLED:
                d_ctrl.central_controller = (controller.addr, controller.aid)
            a.add_role(d_ctrl)
            d_obs = DecentralizedObserverRole(controller=d_ctrl)
            a.observer_module = d_obs
            a.add_role(d_obs)

    for _ in range(NUMBER_OF_HOUSEHOLDS):
        current_container = await create_container(addr=f"household_agent_{household_ids}", codec=codec,
                                                   connection_type='external_connection',
                                                   manipulation_id=ID_AGENT_MAPPING[MANIPULATED_AGENT_ID],
                                                   attack_scenario=ATTACK_SCENARIO)
        a = LoadAgent(current_container, suggested_aid=f'household_agent_{household_ids}',
                      controller_addr=controller_addr, controller_aid=controller_aid)
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
        agent_type_mapping[(a.addr, a.aid)] = AgentType.HOUSEHOLD
        if DECENTRALIZED_CONTROL:
            d_ctrl = DecentralizedController(aggregator=(aggregator_agent.addr, aggregator_agent.aid))
            if MULTI_LEVELLED:
                d_ctrl.central_controller = (controller.addr, controller.aid)
            a.add_role(d_ctrl)
            d_obs = DecentralizedObserverRole(controller=d_ctrl)
            a.observer_module = d_obs
            a.add_role(d_obs)

    mango_communication_network = MangoCommunicationNetwork(client_container_mapping=client_container_mapping,
                                                            port=PORT, codec=codec)
    coalition_initiator_role = CoalitionInitiatorRole(addrs, "cohda", "cohda-negotiation")

    aggregation_role.agent_names = agent_names
    aggregation_role.agent_addrs = addrs
    aggregation_role.cohda_agents = [a.aid for a in cohda_agents]

    if CENTRALIZED_CONTROL or MULTI_LEVELLED:
        controller.agent_names = agent_names
        controller.agent_addrs = addrs
        controller.agent_type_mapping = agent_type_mapping
        controller.coalition_initiator = coalition_initiator_role
        aggregator_agent.addrs = addrs
    if DECENTRALIZED_CONTROL:
        aggregator_agent.addrs = addrs

    start = time.time()
    aggregator_agent.add_role(
        coalition_initiator_role
    )

    for a in cohda_agents + [aggregator_agent]:
        if a._check_inbox_task.done():
            if a._check_inbox_task.exception() is not None:
                raise a._check_inbox_task.exception()
            else:
                assert False, f"check_inbox terminated unexpectedly."

    await aggregation_role.step_done
    time.sleep(2)
    if ATTACK_SCENARIO == ATTACK_TYPE.DELAYS:
        await mango_communication_network.store_falsified_msgs(aggregation_role._current_date_time)
    s = [await container.shutdown() for container in containers]
    mango_communication_network.omnetpp_connection.close_connection()
    try:
        stop_omnet(mango_communication_network.omnet_process)
    except ProcessLookupError:
        pass


def main():
    asyncio.run(redispatch_scenario())


if __name__ == '__main__':
    main()
