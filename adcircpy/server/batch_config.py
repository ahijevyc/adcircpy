from datetime import timedelta
import os
import uuid

from adcircpy.server.base_config import BaseServerConfig


class BatchConfig(BaseServerConfig):
    """
    Object instance of a batch shell script (`*.job`).
    """

    def __init__(
        self,
        account: str,
        ntasks: int,
        walltime: timedelta,
        memory: str = None,
        queue : str = 'main',
        filename: str = 'batch.job',
        run_name: str = None,
        mail_user: str = None,
        modules: [str] = None,
        path_prefix: str = None,
        extra_commands: [str] = None,
        launcher: str = 'mpiexec',
        nodes: int = None,
    ):
        """
        Instantiate a new batch shell script (`*.job`).

        :param account: charge account 
        :param ntasks: number of total tasks to run
        :param memory: memory
        :param run_name: job run name
        :param mail_user: email address
        :param queue : queue to run on
        :param walltime: time delta
        :param driver_script_filename: file path to the driver shell script
        :param modules: list of file paths to modules to load
        :param path_prefix: file path to prepend to the PATH
        :param extra_commands: list of extra shell commands to insert into script
        :param launcher: command to start processes on target system (`srun`, `ibrun`, etc.)
        :param nodes: number of total nodes
        """
        self._account = account
        self._ntasks = ntasks
        self._memory = memory
        self._run_name = run_name
        self._mail_user = mail_user
        self._queue = queue 
        self._walltime = walltime
        self._filename = filename
        self._modules = modules
        self._path_prefix = path_prefix
        self._extra_commands = extra_commands
        self._launcher = launcher
        self._nodes = nodes

    @property
    def nprocs(self):
        return self._ntasks * self._nodes

    @property
    def _walltime(self):
        return self.__walltime

    @_walltime.setter
    def _walltime(self, walltime):
        hours, remainder = divmod(walltime, timedelta(hours=1))
        minutes, remainder = divmod(remainder, timedelta(minutes=1))
        seconds = round(remainder / timedelta(seconds=1))
        self.__walltime = f'{hours:02}:{minutes:02}:{seconds:02}'

    @property
    def _filename(self):
        return self.__filename

    @_filename.setter
    def _filename(self, filename):
        if filename is None:
            filename = 'batch.job'
        self.__filename = filename

    @property
    def _run_name(self):
        return self.__run_name

    @_run_name.setter
    def _run_name(self, run_name):
        if run_name is None:
            run_name = uuid.uuid4().hex
        self.__run_name = run_name

    @property
    def _prefix(self):
        f = f'#PBS -N {self._run_name}\n'

        if self._account is not None:
            f += f'#PBS -A {self._account}\n'

        if self._nodes is not None:
            f += f'#PBS -l select={self._nodes}:ncpus={self._ntasks}:mpiprocs={self._ntasks}'
            if self._memory is not None:
                f += f':mem={self._memory}'
            f += '\n'

        f += f'#PBS -j oe\n'
        f += f'#PBS -k eod\n'

        f += f'#PBS -l walltime={self._walltime}\n'


        if self._queue is not None:
            f += f'#PBS -q {self._queue}\n'

        if self._mail_user is not None:
            f += f'#PBS -M {self._mail_user}\n'

        f += f'\n' f'setenv TMPDIR /glade/scratch/{os.getenv("USER")}/temp\n'
        f += f'mkdir -p $TMPDIR\n'
        if self._modules is None:
            f += f'module reset\n'
        else:
            f += f'module purge\n'
            f += f'\n' f'module load {" ".join(module for module in self._modules)}\n'

        if self._path_prefix is not None:
            f += f'\n' f'setenv PATH {self._path_prefix}:$PATH\n'

        if self._extra_commands is not None:
            f += '\n'
            for command in self._extra_commands:
                f += f'{command}\n'

        return f
