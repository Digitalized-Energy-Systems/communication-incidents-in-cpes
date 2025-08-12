import random
from enum import Enum
from os import path
from os.path import abspath
from pathlib import Path

from midas.util.runtime_config import RuntimeConfig

from pysimmods.generator.chplpgsystemsim.presets import chp_preset
class ATTACK_TYPE(Enum):
    NONE = 0
    ASSET_FAILURE = 5
    COMMUNICATION_FAILURE = 6
    DELAYS = 7
    COMPROMISED_DATA = 8
    COMMUNICATION_TO_FIELD = 9
    COMMUNICATION_FROM_FIELD = 10

# data paths
ROOT_PATH = Path(abspath(__file__)).parent.parent
data_path = ROOT_PATH / 'data'
WIND_DATA = data_path / 'wind_speed_2020.csv'
POWER_CURVE_DATA_FILE = ROOT_PATH / 'power_coeff_curve.csv'
PV_DATA = data_path / 'pv_10kw.csv'
ROOT_PATH_midas = Path(abspath(__file__)).parent
data_path_midas = ROOT_PATH_midas / 'data'
MIDAS_DATA = data_path_midas / 'midas_data' / RuntimeConfig().data["simbench"][0]["name"].replace('csv', 'hdf5')

# scenario configuration
STEP_SIZE = 900
NUM_SIMULATIONS = 2
# Maximum Wind at this time
# 11:45 - 13:00
START = '2022-07-23 12:00:00'
START_FORMAT_CSV = '23.07.2022 12:00:00'
END = '2022-07-23 12:00:00'
SIMULATION_HOURS = 6
SIMULATION_HOURS_IN_RESOLUTION = int(SIMULATION_HOURS * 4)
NUMBER_OF_SCHEDULES_PER_AGENT = 10
# 2 Wind, 3 PV, 2 CHP, 3 Batteries,
# 10 Households
NUMBER_OF_WIND_AGENTS = 2
NUMBER_OF_PV_AGENTS = 3
NUMBER_OF_CHPS = 2
NUMBER_OF_BATTERIES = 3
NUMBER_OF_HOUSEHOLDS = 10

NUMBER_OF_AGENTS_TOTAL = NUMBER_OF_WIND_AGENTS + NUMBER_OF_CHPS + NUMBER_OF_PV_AGENTS + NUMBER_OF_BATTERIES + NUMBER_OF_HOUSEHOLDS

NEGOTIATION_TIMEOUT = 200

CENTRALIZED_CONTROL = True
DECENTRALIZED_CONTROL = False
MULTI_LEVELLED = False

db_file = 'results' + START + '.hdf5'
ID_AGENT_MAPPING = {'0': 'aggregator_agent',
                    '1': 'generation_agent_0',
                    '2':  'generation_agent_1',
                    '3': 'generation_agent_2',
                    '4': 'generation_agent_3',
                    '5': 'generation_agent_4',
                    '6':'generation_agent_5',
                    '7': 'generation_agent_6',
                    '8': 'storage_agent_0',
                    '9': 'storage_agent_1',
                    '10': 'storage_agent_2',
                    '11': 'household_agent_0',
                    '12': 'household_agent_1',
                    '13': 'household_agent_2',
                    '14': 'household_agent_3',
                    '15': 'household_agent_4',
                    '16': 'household_agent_5',
                    '17': 'household_agent_6',
                    '18': 'household_agent_7',
                    '19': 'household_agent_8',
                    '20': 'household_agent_9'
                    }
# 0: No Failure
# 5: Asset failure
# 6: Communication failure
# 7. Communication delays
# 8. Compromised process data
# 9. No communication from Aggregator Agent to neighborhood grid
# 10. No communication from neighborhood grid to Aggregator agent
ATTACK_SCENARIO = ATTACK_TYPE.NONE.value
# choose partition id of the agent for attacks 0-4 and 8
# choose name of the agent for attacks 5, 6, 7 (agent id)
# set to aggregator_agent for 9, 10

generation_agents_names = ['generation_agent_0', 'generation_agent_1', 'generation_agent_2', 'generation_agent_3',
                           'generation_agent_4', 'generation_agent_5', 'generation_agent_6']
generation_agents_ids = ['1', '2', '3', '4', '5', '6', '7']
storage_agents_names = ['storage_agent_0', 'storage_agent_1', 'storage_agent_2']
storage_agents_ids = ['8', '9', '10']
household_agents_names = ['household_agent_0', 'household_agent_1', 'household_agent_2', 'household_agent_3',
                          'household_agent_4', 'household_agent_5', 'household_agent_6', 'household_agent_7',
                          'household_agent_8', 'household_agent_9']
household_agents_ids = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20']
aggregator_agent_name = 'aggregator_agent'

MANIPULATED_AGENT_ID = '8'#'generation_agent_2'  # random.choice(household_agents_ids)

SCHEDULE_PERCENTAGES = [0.2, 0.4, 0.6, 0.8, 1.0]

# UNIT PARAMETERS
PARAMS_BATT = {
    'cap_kwh': .05,
    'p_charge_max_kw': .1,
    'p_discharge_max_kw': .1,
    'soc_min_percent': 15,
    'eta_pc': [-2.109566, 0.403556, 97.110770],
}
INITS_BATT = {
    'soc_percent': 50
}

POSSIBLE_KW = [7, 14, 200, 400]
CHP_PARAMS, CHP_INITS = chp_preset(200)  # It is not working for 150 kW

# weather information
T_AIR = "WeatherCurrent__0___t_air_deg_celsius"
WIND = "WeatherCurrent__0___wind_v_m_per_s"
WIND_DIR = "WeatherCurrent__0___wind_dir_deg"
PRESSURE = "WeatherCurrent__0___air_pressure_hpa"
WD_PATH = path.abspath(
    path.join(__file__, "..", "data", "weather-time-series.csv")
)

BH = "WeatherCurrent__0___bh_w_per_m2"
DH = "WeatherCurrent__0___dh_w_per_m2"

INITIAL_TARGET_PARAMS = {
    'current_start_date': START}
