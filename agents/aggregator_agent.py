from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from pysimmods.other.flexibility.flexibilities import Flexibilities
from pysimmods.other.flexibility.flexibility_model import FlexibilityModel
from pysimmods.other.flexibility.schedule import Schedule
from pysimmods.util.date_util import GER


class AdaptedFlexibilityModel(FlexibilityModel):

    def __init__(
            self,
            model,
            unit="kw",
            prioritize_setpoint: bool = False,
            step_size: Optional[int] = None,
            now_dt: Optional[datetime] = None,
            forecast_horizon_hours=1,
            seed=None,
            store_min_and_max: bool = False,
    ):
        super().__init__(model=model, unit=unit, prioritize_setpoint=prioritize_setpoint,
                         step_size=step_size, now_dt=now_dt, forecast_horizon_hours=forecast_horizon_hours,
                         seed=seed, store_min_and_max=store_min_and_max)

    def maximum_flex(self, start, flexibility_horizon_hours):
        step_size = self._step_size

        periods = int(flexibility_horizon_hours * 3_600 / step_size)

        start_dt = datetime.strptime(start, GER)
        end_dt = (
                start_dt
                + timedelta(hours=flexibility_horizon_hours)
                - timedelta(seconds=step_size)
        )
        index = pd.date_range(start_dt, end_dt, periods=periods)
        dataframe = pd.DataFrame(
            columns=[
                self._psetname,
                self._qsetname,
                self._pname,
                self._qname,
            ],
            index=index,
        )
        dataframe[self._psetname] = [self.get_pn_max_kw() for _ in range(len(index))]
        dataframe[self._qsetname] = [self.get_qn_max_kvar() for _ in range(len(index))]

        state_backup = self._model.get_state()
        for index, row in dataframe.iterrows():
            try:
                self._calculate_step(
                    index, row[self._psetname], row[self._qsetname]
                )
                dataframe.loc[index, self._pname] = (
                        self._model.get_p_kw() * self._unit_factor
                )
                dataframe.loc[index, self._qname] = (
                        self._model.get_q_kvar() * self._unit_factor
                )

            except KeyError:
                # Forecast is missing
                dataframe.loc[index, self._pname] = np.nan
                dataframe.loc[index, self._qname] = np.nan
                dataframe.loc[index, self._psetname] = np.nan
                dataframe.loc[index, self._qsetname] = np.nan
        self._model.set_state(state_backup)
        return Schedule().from_dataframe(dataframe)

    def minimum_flex(self, start, flexibility_horizon_hours):
        step_size = self._step_size

        periods = int(flexibility_horizon_hours * 3_600 / step_size)

        start_dt = datetime.strptime(start, GER)
        end_dt = (
                start_dt
                + timedelta(hours=flexibility_horizon_hours)
                - timedelta(seconds=step_size)
        )
        index = pd.date_range(start_dt, end_dt, periods=periods)
        dataframe = pd.DataFrame(
            columns=[
                self._psetname,
                self._qsetname,
                self._pname,
                self._qname,
            ],
            index=index,
        )
        dataframe[self._psetname] = [self.get_pn_min_kw() for _ in range(len(index))]
        dataframe[self._qsetname] = [self.get_qn_min_kvar() for _ in range(len(index))]

        state_backup = self._model.get_state()
        for index, row in dataframe.iterrows():
            try:
                self._calculate_step(
                    index, row[self._psetname], row[self._qsetname]
                )
                dataframe.loc[index, self._pname] = (
                        self._model.get_p_kw() * self._unit_factor
                )
                dataframe.loc[index, self._qname] = (
                        self._model.get_q_kvar() * self._unit_factor
                )

            except KeyError:
                # Forecast is missing
                dataframe.loc[index, self._pname] = np.nan
                dataframe.loc[index, self._qname] = np.nan
                dataframe.loc[index, self._psetname] = np.nan
                dataframe.loc[index, self._qsetname] = np.nan
        self._model.set_state(state_backup)
        return Schedule().from_dataframe(dataframe)
