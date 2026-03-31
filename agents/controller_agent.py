import uuid

import h5py
import numpy as np
from h5py.h5fd import MULTI

from agents.aggregation_role import AggregationRole
from config import ATTACK_SCENARIO, ATTACK_TYPE, INITIAL_TARGET_PARAMS, NUMBER_OF_AGENTS_TOTAL, NEGOTIATION_TIMEOUT, \
    ID_AGENT_MAPPING, MANIPULATED_AGENT_ID, CENTRALIZED_CONTROL, MULTI_LEVELLED
from mango import RoleAgent

from agents.messages import CoalitionAdaption, ReassignRole, CallForExclusion, CallForNewTopology, Inactive
from mango_library.coalition.core import CoalitionAssignmentConfirm, small_world_creator
from mango_library.negotiation.cohda.cohda_messages import StartCohdaNegotiationMessage
from mango_library.negotiation.cohda.cohda_starting import CohdaNegotiationInteractiveStarterRole
from util import AgentType



class CentralizedControllerAgent(RoleAgent):

    def __init__(self, container, suggested_aid, n_agents):
        super().__init__(container, suggested_aid=suggested_aid)
        self.agent_names = []
        self.agent_addrs = []
        self.agent_type_mapping = {}
        self.coalition_initiator = None
        self.topology = {}
        self.participants = None
        self.coalition_controller_agent_id = None
        self.coalition_controller_agent_addr = None
        self.aggregation_role = None
        self.container = container
        self._hf = None
        self._n_agents = n_agents
        self.observer_module = None
        self.unit_roles = None

    def store_msg_to_db(self, content, m_id):
        current_time = self.container.clock.time
        self._hf = h5py.File(f'{self.aid}_rec_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))
        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)
        general_group.create_dataset('time', data=np.float64(current_time))
        general_group.attrs["aid"] = self.aid
        self._hf.close()

    def handle_message(self, content, meta):
        super().handle_message(content, meta)
        print(
            f"CentralizedControllerAgent {self.aid} received a message with the following content: {type(content)}")  # , at: {self._container.clock.time}")
        if isinstance(content, CoalitionAssignmentConfirm):
            self.participants = self.coalition_initiator.accepted_participants
            self.coalition_done(self.coalition_initiator.part_to_neighbors)
            self.coalition_controller_agent_id = meta["sender_id"]
            self.coalition_controller_agent_addr = meta["sender_addr"]

        if isinstance(content, Inactive):
            self.schedule_instant_acl_message(Inactive(),
                                              receiver_addr=self.aggregation_role._context.addr,
                                              receiver_id=self.aggregation_role._context.aid,
                                              acl_metadata={
                                                  "sender_addr": self._context.addr,
                                                  "sender_id": self.aid,
                                                  "conversation_id": str(uuid.uuid4())
                                              }
                                              )

            self.anomaly_found(ID_AGENT_MAPPING[MANIPULATED_AGENT_ID])

        if isinstance(content,
                      StartCohdaNegotiationMessage) and ATTACK_SCENARIO != ATTACK_TYPE.COMMUNICATION_TO_FIELD.value:
            if self.observer_module and CENTRALIZED_CONTROL:
                self.observer_module.start_listening()
        if isinstance(content, CallForNewTopology):
            self.create_new_topology(content)
        self.store_msg_to_db(content, meta['conversation_id'])

    def create_new_topology(self, content):
        participants = []
        for part in self.participants:
            if part[1] != content.malicious_agent:
                participants.append(part)
        self.participants = participants

        part_to_neighbors = small_world_creator(
            self.participants,
        )
        self.coalition_done(part_to_neighbors)
        for part in self.participants:
            content = CoalitionAdaption(
                coalition_id=self.coalition_initiator._coal_id,
                neighbors=part_to_neighbors[part],
                topic=self.coalition_initiator._topic,
                part_id=part[0],
                controller_agent_id=self.coalition_controller_agent_id,
                controller_agent_addr=self.coalition_controller_agent_addr,
                malicious_agent=content.malicious_agent
            )
            self.schedule_instant_acl_message(
                content=content,
                receiver_addr=part[1],
                receiver_id=part[2],
                acl_metadata={
                    "sender_addr": self._context.addr,
                    "sender_id": self.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )
        aggr_info = self.get_addr_for_type(agent_type=AgentType.AGGREGATOR)

        # inform aggregator
        content = CoalitionAdaption(
            coalition_id=self.coalition_initiator._coal_id,
            neighbors=part_to_neighbors[part],
            topic=self.coalition_initiator._topic,
            part_id=part[0],
            controller_agent_id=self.coalition_controller_agent_id,
            controller_agent_addr=self.coalition_controller_agent_addr,
            malicious_agent=content.malicious_agent
        )
        self.schedule_instant_acl_message(
            content=content,
            receiver_addr=aggr_info[0],
            receiver_id=aggr_info[1],
            acl_metadata={
                "sender_addr": self._context.addr,
                "sender_id": self.aid,
                "conversation_id": str(uuid.uuid4())
            },
        )

        if MULTI_LEVELLED:
            return
        malicious_agent_type = self.agent_type_mapping[(content.malicious_agent, content.malicious_agent)]
        for agent, agent_type in self.agent_type_mapping.items():
            if ATTACK_SCENARIO == ATTACK_TYPE.COMMUNICATION_FROM_FIELD.value:
                if agent[
                    0] != content.malicious_agent and agent_type != AgentType.AGGREGATOR and agent_type != AgentType.OPERATOR:
                    # only forward role of communication to aggregator
                    role = CohdaNegotiationInteractiveStarterRole.__name__
                    content = ReassignRole(
                        roles_and_params={role: [INITIAL_TARGET_PARAMS]}
                    )
                    self.schedule_instant_acl_message(
                        content=content,
                        receiver_addr=agent[0],
                        receiver_id=agent[1],
                        acl_metadata={
                            "sender_addr": self._context.addr,
                            "sender_id": self.aid,
                            "conversation_id": str(uuid.uuid4())
                        },
                    )

                    return
            else:
                if agent_type == malicious_agent_type:
                    if agent[0] == content.malicious_agent:
                        continue
                    else:
                        role = self.unit_roles[content.malicious_agent]
                        content = ReassignRole(
                            roles_and_params={role: [agent[0],
                                                     self.aid, content.malicious_agent]}
                        )
                        self.schedule_instant_acl_message(
                            content=content,
                            receiver_addr=agent[0],
                            receiver_id=agent[1],
                            acl_metadata={
                                "sender_addr": self._context.addr,
                                "sender_id": self.aid,
                                "conversation_id": str(uuid.uuid4())
                            },
                        )

                        return

    def coalition_done(self, part_to_neighbors):
        # store current topology
        for part_id, neighbors in part_to_neighbors.items():
            for entry in self.participants:
                if entry[0] == part_id[0]:
                    agent_info = (entry[1], entry[2])
                    self.topology[agent_info] = neighbors

    def anomaly_found(self, malicious_agent, incident_type=None, data=None):
        self.exclude_malicious_agent(malicious_agent)
        if malicious_agent == AgentType.AGGREGATOR.name:
            self.replace_aggregator(malicious_agent)
        else:
            malicious_agent_type = self.agent_type_mapping[(malicious_agent, malicious_agent)]
            for agent, agent_type in self.agent_type_mapping.items():
                if ATTACK_SCENARIO == ATTACK_TYPE.COMMUNICATION_FROM_FIELD.value:
                    if agent[
                        0] != malicious_agent and agent_type != AgentType.AGGREGATOR and agent_type != AgentType.OPERATOR:
                        # only forward role of communication to aggregator
                        role = CohdaNegotiationInteractiveStarterRole.__name__

                        content = ReassignRole(
                            roles_and_params={role: [INITIAL_TARGET_PARAMS]}
                        )
                        self.schedule_instant_acl_message(
                            content=content,
                            receiver_addr=agent[0],
                            receiver_id=agent[1],
                            acl_metadata={
                                "sender_addr": self._context.addr,
                                "sender_id": self.aid,
                                "conversation_id": str(uuid.uuid4())
                            },
                        )

                        return
                else:
                    if agent_type == malicious_agent_type:
                        if agent[0] == malicious_agent:
                            continue
                        else:
                            role = self.unit_roles[malicious_agent]
                            content = ReassignRole(
                                roles_and_params={role: [agent[0],
                                self.aid, malicious_agent]}
                            )
                            self.schedule_instant_acl_message(
                                content=content,
                                receiver_addr=agent[0],
                                receiver_id=agent[1],
                                acl_metadata={
                                    "sender_addr": self._context.addr,
                                    "sender_id": self.aid,
                                    "conversation_id": str(uuid.uuid4())
                                },
                            )

                            return

    def replace_aggregator(self, malicious_agent):
        part_map = self.aggregation_role.term_detector._participant_map
        final_part_map = {}
        for entry, s_items in part_map.items():
            final_part_map[str(entry)] = list(s_items)
        content = ReassignRole(
            roles_and_params={
                AggregationRole.__name__: [NUMBER_OF_AGENTS_TOTAL, NEGOTIATION_TIMEOUT, INITIAL_TARGET_PARAMS,
                                           (self.addr, self.aid)]},
            additional_params={'agent_addrs': self.agent_addrs,
                               'participant_map': final_part_map}
        )
        new_role_manager = (self.participants[0][1], self.participants[0][2])
        self.schedule_instant_acl_message(
            content=content,
            receiver_addr=new_role_manager[0],
            receiver_id=new_role_manager[1],
            acl_metadata={
                "sender_addr": self._context.addr,
                "sender_id": self.aid,
                "conversation_id": str(uuid.uuid4())
            },
        )
        agent_name = 'aggregator_agent'
        if (agent_name, agent_name) in self.agent_addrs:
            idx = self.agent_addrs.index((agent_name, agent_name))
            del self.agent_addrs[idx]
        if agent_name in self.agent_names:
            idx = self.agent_names.index(agent_name)
            del self.agent_names[idx]
        if (agent_name, agent_name) in self.agent_type_mapping.keys():
            del self.agent_type_mapping[(agent_name, agent_name)]
        self.agent_type_mapping[new_role_manager] = [self.agent_type_mapping[new_role_manager], AgentType.AGGREGATOR]

        # send coalition to new aggregator
        self.coalition_controller_agent_id = new_role_manager[0]
        self.coalition_controller_agent_addr = new_role_manager[1]
        aggr_info = self.get_addr_for_type(agent_type=AgentType.AGGREGATOR)
        # inform new aggregator
        content = CoalitionAdaption(
            coalition_id=str(self.coalition_initiator._coal_id),
            neighbors=[],
            topic=self.coalition_initiator._topic,
            part_id=[],
            controller_agent_id=self.coalition_controller_agent_id,
            controller_agent_addr=self.coalition_controller_agent_addr,
            malicious_agent=malicious_agent
        )

        self.schedule_instant_acl_message(
            content=content,
            receiver_addr=aggr_info[0],
            receiver_id=aggr_info[1],
            acl_metadata={
                "sender_addr": self._context.addr,
                "sender_id": self.aid,
                "conversation_id": str(uuid.uuid4())
            },
        )
        for part in self.participants:
            if part[1] == aggr_info[1]:
                continue
            content = CoalitionAdaption(
                coalition_id=str(self.coalition_initiator._coal_id),
                neighbors=[],
                topic=self.coalition_initiator._topic,
                part_id=[],
                controller_agent_id=self.coalition_controller_agent_id,
                controller_agent_addr=self.coalition_controller_agent_addr,
                malicious_agent=malicious_agent
            )
            self.schedule_instant_acl_message(
                content=content,
                receiver_addr=part[1],
                receiver_id=part[2],
                acl_metadata={
                    "sender_addr": self._context.addr,
                    "sender_id": self.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )

    def exclude_malicious_agent(self, malicious_agent):
        if malicious_agent == AgentType.AGGREGATOR.name:
            for part in self.participants:
                content = CallForExclusion(
                    coalition_id=self.coalition_initiator._coal_id,
                    malicious_agent=malicious_agent
                )

                self.schedule_instant_acl_message(
                    content=content,
                    receiver_addr=part[1],
                    receiver_id=part[2],
                    acl_metadata={
                        "sender_addr": self._context.addr,
                        "sender_id": self.aid,
                        "conversation_id": str(uuid.uuid4())
                    },
                )
        else:
            if len(self.participants) == (self._n_agents - 1):
                return
            self._n_agents -= 1
            participants = []
            for part in self.participants:
                if part[1] != malicious_agent:
                    participants.append(part)
            self.participants = participants

            part_to_neighbors = small_world_creator(
                self.participants,
            )
            self.coalition_done(part_to_neighbors)
            for part in self.participants:
                content = CoalitionAdaption(
                    coalition_id=self.coalition_initiator._coal_id,
                    neighbors=part_to_neighbors[part],
                    topic=self.coalition_initiator._topic,
                    part_id=part[0],
                    controller_agent_id=self.coalition_controller_agent_id,
                    controller_agent_addr=self.coalition_controller_agent_addr,
                    malicious_agent=malicious_agent
                )
                self.schedule_instant_acl_message(
                    content=content,
                    receiver_addr=part[1],
                    receiver_id=part[2],
                    acl_metadata={
                        "sender_addr": self._context.addr,
                        "sender_id": self.aid,
                        "conversation_id": str(uuid.uuid4())
                    },
                )

            aggr_info = self.get_addr_for_type(agent_type=AgentType.AGGREGATOR)

            # inform aggregator
            content = CoalitionAdaption(
                coalition_id=self.coalition_initiator._coal_id,
                neighbors=part_to_neighbors[part],
                topic=self.coalition_initiator._topic,
                part_id=part[0],
                controller_agent_id=self.coalition_controller_agent_id,
                controller_agent_addr=self.coalition_controller_agent_addr,
                malicious_agent=malicious_agent
            )

            self.schedule_instant_acl_message(
                content=content,
                receiver_addr=aggr_info[0],
                receiver_id=aggr_info[1],
                acl_metadata={
                    "sender_addr": self._context.addr,
                    "sender_id": self.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )

    def get_addr_for_type(self, agent_type):
        for agent_info, current_type in self.agent_type_mapping.items():
            if current_type == agent_type:
                return agent_info
            if isinstance(current_type, list):
                for actual_type in current_type:
                    if actual_type == agent_type:
                        return agent_info
