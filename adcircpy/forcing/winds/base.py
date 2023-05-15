from abc import ABC, abstractmethod
from datetime import timedelta
from os import PathLike

from adcircpy.forcing.base import Forcing


class WindForcing(Forcing, ABC):
    def __init__(self, nws: int, interval_seconds: int, spinup_time: timedelta):
        super().__init__(interval_seconds)
        self.NWS = nws
        self.spinup_time = spinup_time

    @abstractmethod
    def write(self, directory: PathLike, overwrite: bool = False):
        raise NotImplementedError

    @classmethod
    def from_fort22(cls, fort22: PathLike, nws: int = None) -> 'WindForcing':
        raise NotImplementedError(f'reading `fort.22` is not implemented for {cls}')
