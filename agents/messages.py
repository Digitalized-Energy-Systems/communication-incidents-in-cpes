from typing import List
from uuid import UUID

from mango.messages.codecs import json_serializable

from mango_library.coalition.core import ParticipantKey, ContainerAddress


@json_serializable
class AggregatedSolutionMessage:
    """
        Message to send an aggregated Solution
        """

    def __init__(self, aggregated_solution, aggregated_flexibility, dates):
        self._aggregated_solution = aggregated_solution
        self._aggregated_flexibility = aggregated_flexibility
        self._dates = dates

    @property
    def aggregated_solution(self):
        return self._aggregated_solution

    @property
    def aggregated_flexibility(self):
        return self._aggregated_flexibility

    @property
    def dates(self):
        return self._dates


@json_serializable
class RedispatchFlexibilityRequest:
    """
    Message to ask agents for redispatch flexibility
    """

    def __init__(self, dates, obligations):
        self._dates = dates
        self._obligations = obligations

    @property
    def dates(self):
        return self._dates

    @property
    def obligations(self):
        return self._obligations


@json_serializable
class RedispatchFlexibilityReply:
    """
    Message for agents to send redispatch flexibility
    """

    def __init__(self, dates, flexibility):
        self._dates = dates
        self._flexibility = flexibility

    @property
    def dates(self):
        return self._dates

    @property
    def flexibility(self):
        return self._flexibility


@json_serializable
class CallForAdaption:
    """
        Message to send for call for adaption in scheduled power values
        """

    def __init__(self, obligation_date, obligation_value):
        self._obligation_date = obligation_date
        self._obligation_value = obligation_value

    @property
    def obligation_date(self):
        return self._obligation_date

    @property
    def obligation_value(self):
        return self._obligation_value


@json_serializable
class CoalitionAdaption:
    """Message/Model for assigning a participant to an already accepted coalition. In this
    assignment all relevant information about the coalition are contained,
    f.e. participant id, neighbors, ... .
    """

    def __init__(
            self,
            coalition_id: UUID,
            malicious_agent: str,
            neighbors: List[ParticipantKey] = None,
            topic: str=None,
            part_id: str=None,
            controller_agent_id: str=None,
            controller_agent_addr=None,
    ):
        self._coalition_id = coalition_id
        self._neighbors = neighbors
        self._topic = topic
        self._part_id = part_id
        self._controller_agent_id = controller_agent_id
        self._controller_agent_addr = controller_agent_addr
        self._malicious_agent = malicious_agent

    @property
    def coalition_id(self) -> UUID:
        """Id of the colaition (unique)

        :return: id of the coalition as UUID
        """
        return self._coalition_id

    @property
    def neighbors(self) -> List[ParticipantKey]:
        """Neighbors of the participant.

        :return: List of the participant, a participant is modelled as
                                        tuple (part_id, address, aid)
        """
        return self._neighbors

    @property
    def topic(self):
        """The topic of the coalition, f.e. COHDA

        :return: the topic
        """
        return self._topic

    @property
    def part_id(self) -> str:
        """The id of the participant

        :return: id
        """
        return self._part_id

    @property
    def controller_agent_id(self):
        """Id of the controller agent

        :return: agent_id
        """
        return self._controller_agent_id

    @property
    def controller_agent_addr(self) -> ContainerAddress:
        """Adress of the controller agent

        :return: adress as tuple
        """
        return self._controller_agent_addr

    @property
    def malicious_agent(self):
        return self._malicious_agent


@json_serializable
class CallForExclusion:
    """
    Message to send for call for exclusion to exclude a malicious agent
    """

    def __init__(self, malicious_agent, coalition_id):
        self._malicious_agent = malicious_agent
        self._coalition_id = coalition_id

    @property
    def malicious_agent(self):
        return self._malicious_agent

    @property
    def coalition_id(self):
        return self._coalition_id


@json_serializable(
)
class Inactive:

    def __init__(self, additional_params=None):
        self.additional_params = additional_params

@json_serializable
class ReassignRole:
    """
    Message to reassign task to another agent
    """

    def __init__(self, roles_and_params, additional_params=None):
        self._roles_and_params = roles_and_params
        self._additional_params = additional_params

    @property
    def roles_and_params(self):
        return self._roles_and_params

    @property
    def additional_params(self):
        return self._additional_params

@json_serializable
class CallForNewTopology:
    """
        Message to send for call for adaption in scheduled power values
        """

    def __init__(self, coalition_id, malicious_agent):
        self._coalition_id = coalition_id
        self._malicious_agent = malicious_agent

    @property
    def coalition_id(self):
        return self._coalition_id

    @property
    def malicious_agent(self):
        return self._malicious_agent
