import uuid

import h5py
import numpy as np

from agents.messages import CallForExclusion, CoalitionAdaption, CallForNewTopology, ReassignRole
from agents.unit_roles import LoadFlexibilityRole
from config import ATTACK_TYPE, ATTACK_SCENARIO, INITIAL_TARGET_PARAMS, MULTI_LEVELLED
from mango import Role
from mango_library.coalition.core import CoalitionAssignment, CoalitionModel
from mango_library.negotiation.cohda.cohda_starting import CohdaNegotiationInteractiveStarterRole

class DecentralizedController(Role):

    def __init__(self, aggregator, multi_levelled=False, container=None):
        super().__init__()
        self.aggregator = aggregator
        self.central_controller = None
        self.multi_levelled = multi_levelled
        self.container = container
        self.done = False

    def store_msg_to_db(self, content, m_id):
        current_time = self.self._context._role_handler._agent_context._container.clock.time
        self._hf = h5py.File(f'{self._context.aid}_rec_msg.h5', 'a')
        try:
            general_group = self._hf.create_group(f'{current_time}')
        except ValueError:
            general_group = self._hf.create_group(f'{current_time}_{str(uuid.uuid4())}')
        self._hf.attrs['content'] = str(type(content))
        general_group.attrs['content'] = str(type(content))
        self._hf.attrs['m_id'] = str(m_id)
        general_group.attrs['m_id'] = str(m_id)
        general_group.create_dataset('time', data=np.float64(current_time))
        general_group.attrs["aid"] = self._context.aid
        self._hf.close()

    def exclude_malicious_agent(self, coalition_id, malicious_agent):
        if ATTACK_SCENARIO == ATTACK_TYPE.COMMUNICATION_FROM_FIELD.value:
            # current_container
            return
        coalition_assignment: CoalitionAssignment = self.context.get_or_create_model(
            CoalitionModel
        ).by_id(coalition_id)

        for idx, neighbor in enumerate(coalition_assignment.neighbors):
            if neighbor[1] == malicious_agent or neighbor[2] == malicious_agent:
                del coalition_assignment.neighbors[idx]
                break
        self.context.update(coalition_assignment)
        for neighbor in coalition_assignment.neighbors:
            self.context.schedule_instant_acl_message(
                content=CallForExclusion(
                    malicious_agent=malicious_agent,
                    coalition_id=coalition_id
                ),
                receiver_addr=neighbor[1],
                receiver_id=neighbor[2],
                acl_metadata={
                    "sender_addr": self.context.addr,
                    "sender_id": self.context.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )
        coalition_assignment: CoalitionAssignment = self.context.get_or_create_model(
            CoalitionModel
        ).by_id(coalition_id)

        # hybrid setting
        if self.central_controller:
            content = CallForNewTopology(
                malicious_agent=malicious_agent,
                coalition_id=coalition_id,
            )
            self.context.schedule_instant_acl_message(
                content=content,
                receiver_addr=self.central_controller[0],
                receiver_id=self.central_controller[1],
                acl_metadata={
                    "sender_addr": self.context.addr,
                    "sender_id": self.context.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )

            if MULTI_LEVELLED:
                # reassign role to self
                if malicious_agent.startswith(self.context.aid[0:5]):
                    if self.done:
                        return
                    new_role = LoadFlexibilityRole(container=self.container, prev_aid=malicious_agent)
                    for role in self._context._role_handler.roles:
                        if isinstance(role, LoadFlexibilityRole):
                            role.additional_control_units.append(new_role)
                            self.done = True
                            return

        else:
            self.context.schedule_instant_acl_message(
                content=CoalitionAdaption(
                    malicious_agent=malicious_agent,
                    coalition_id=coalition_id,
                ),
                receiver_addr=self.aggregator[0],
                receiver_id=self.aggregator[1],
                acl_metadata={
                    "sender_addr": self.context.addr,
                    "sender_id": self.context.aid,
                    "conversation_id": str(uuid.uuid4())
                },
            )
            # reassign role to self
            if malicious_agent.startswith(self.context.aid[0:5]):
                if self.done:
                    return
                new_role = LoadFlexibilityRole(container=self.container, prev_aid=malicious_agent)
                for role in self._context._role_handler.roles:
                    if isinstance(role, LoadFlexibilityRole):
                        role.additional_control_units.append(new_role)
                        self.done = True
                        return
        if ATTACK_SCENARIO == ATTACK_TYPE.COMMUNICATION_FROM_FIELD.value:
            # current_container
            role = CohdaNegotiationInteractiveStarterRole(target_params=INITIAL_TARGET_PARAMS,
                                                          container=self.container)

            for neighbor in coalition_assignment.neighbors:
                self.context.schedule_instant_acl_message(
                    content=ReassignRole(
                        roles=[role]
                    ),
                    receiver_addr=neighbor[1],
                    receiver_id=neighbor[2],
                    acl_metadata={
                        "sender_addr": self.context.addr,
                        "sender_id": self.context.aid,
                        "conversation_id": str(uuid.uuid4())
                    },
                )
