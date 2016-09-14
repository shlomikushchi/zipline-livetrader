# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from abc import abstractproperty

from zipline.data.bar_reader import BarReader


class NoDataBeforeDate(Exception):
    pass


class NoDataAfterDate(Exception):
    pass


class SessionBarReader(BarReader):
    """
    Reader for OHCLV pricing data at a session frequency.
    """
    _data_frequency = 'session'

    @abstractproperty
    def sessions(self):
        """
        Returns
        -------
        sessions : DatetimeIndex
           All session labels (unionining the range for all assets) which the
           reader can provide.
        """
        pass
