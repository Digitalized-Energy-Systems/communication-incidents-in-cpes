This repository allows to investigate communication incidents in Cyber-Physical Energy Systems (CPES). The implementation combines the agent framework mango and the communication simulator OMNeT++.
The scenario consists of an agent system building a neighborhood grid and performing self-consumption optimization. The results
of the optimization are forwarded via an Aggregator Agent to the Grid Operator Agent, which is responsible to check the results regarding
grid or market constraints. If any adaptions are necessary, these are forwarded via the Aggregator Agent to the neighborhood grid.
The neighborhood grid then again performs a self-consumption optimization, taking into account the control signals of the Grid Operator Agent.
The communication network was created using [trace](https://github.com/OFFIS-DAI/trace).

The implementation integrates the consideration of different communication incidents: Asset failure (an agent stops to respond to others),
communication failure (the communication between field and control center fails or vice versa), communication delay (data is transferred slower than usual)
compromised process data (measurements are compromised, for example).

Now install OMNeT++ and cosima according to the [instructions](https://github.com/OFFIS-DAI/cosima/blob/master/README.md). For further information, see [the cosima website](https://cosima.offis.de/).
To consider the correct communication network, check out the branch [communication robustness](https://github.com/OFFIS-DAI/cosima/tree/communication-robustness).

After installing the respective packages, the scenario setting can be done in `config.py`. The date, number of agents and incident to simulate can be set there.
Additionally, different types of control can be activated to react to the incidents, which can also be set in the `config.py`.

To run the scenario with functionalities capsulated into the agents, execute `scenario.py`.
To run the scenario with additional funtionalities, including reactions to incidents (as taking over the tasks of other agents), execute `scenario_roles.py`.
For another scenario for agent-based optimization, the `unit_control_scenario.py` can be executed. For this scenario, two variants of asset failure can be investigated: the failure of agents and sending zero values.

After a successful run, a results file is created, named according to the simulation start date.
