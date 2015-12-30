#
# Copyright 2014 Quantopian, Inc.
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

import os
from nose_parameterized import parameterized
from unittest import TestCase
from testfixtures import TempDirectory
import pandas as pd

import zipline.utils.factory as factory

from zipline.finance import trading
from zipline.finance.blotter import Blotter
from zipline.finance.order import ORDER_STATUS
from zipline.finance.execution import (
    LimitOrder,
    MarketOrder,
    StopLimitOrder,
    StopOrder,
)

from zipline.utils.test_utils import(
    setup_logger,
    teardown_logger,
)
from zipline.finance.slippage import DEFAULT_VOLUME_SLIPPAGE_BAR_LIMIT, \
    FixedSlippage
from .utils.daily_bar_writer import DailyBarWriterFromDataFrames
from zipline.data.us_equity_pricing import BcolzDailyBarReader
from zipline.data.data_portal import DataPortal


class BlotterTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        setup_logger(cls)
        cls.env = trading.TradingEnvironment()

        cls.sim_params = factory.create_simulation_parameters(
            start=pd.Timestamp("2006-01-05", tz='UTC'),
            end=pd.Timestamp("2006-01-06", tz='UTC')
        )

        cls.env.write_data(equities_data={
            24: {
                'start_date': cls.sim_params.trading_days[0],
                'end_date': cls.sim_params.trading_days[-1]
            }
        })

        cls.tempdir = TempDirectory()

        assets = {
            24: pd.DataFrame({
                "open": [50, 50],
                "high": [50, 50],
                "low": [50, 50],
                "close": [50, 50],
                "volume": [100, 400],
                "day": [day.value for day in cls.sim_params.trading_days]
            })
        }

        path = os.path.join(cls.tempdir.path, "tempdata.bcolz")

        DailyBarWriterFromDataFrames(assets).write(
            path,
            cls.sim_params.trading_days,
            assets
        )

        equity_daily_reader = BcolzDailyBarReader(path)

        cls.data_portal = DataPortal(
            cls.env,
            equity_daily_reader=equity_daily_reader,
        )

    @classmethod
    def tearDownClass(cls):
        del cls.env
        cls.tempdir.cleanup()
        teardown_logger(cls)

    @parameterized.expand([(MarketOrder(), None, None),
                           (LimitOrder(10), 10, None),
                           (StopOrder(10), None, 10),
                           (StopLimitOrder(10, 20), 10, 20)])
    def test_blotter_order_types(self, style_obj, expected_lmt, expected_stp):

        blotter = Blotter('daily')

        blotter.order(24, 100, style_obj)
        result = blotter.open_orders[24][0]

        self.assertEqual(result.limit, expected_lmt)
        self.assertEqual(result.stop, expected_stp)

    def test_order_rejection(self):
        blotter = Blotter(self.sim_params.data_frequency)

        # Reject a nonexistent order -> no order appears in new_order,
        # no exceptions raised out
        blotter.reject(56)
        self.assertEqual(blotter.new_orders, [])

        # Basic tests of open order behavior
        open_order_id = blotter.order(24, 100, MarketOrder())
        second_order_id = blotter.order(24, 50, MarketOrder())
        self.assertEqual(len(blotter.open_orders[24]), 2)
        open_order = blotter.open_orders[24][0]
        self.assertEqual(open_order.status, ORDER_STATUS.OPEN)
        self.assertEqual(open_order.id, open_order_id)
        self.assertIn(open_order, blotter.new_orders)

        # Reject that order immediately (same bar, i.e. still in new_orders)
        blotter.reject(open_order_id)
        self.assertEqual(len(blotter.new_orders), 2)
        self.assertEqual(len(blotter.open_orders[24]), 1)
        still_open_order = blotter.new_orders[0]
        self.assertEqual(still_open_order.id, second_order_id)
        self.assertEqual(still_open_order.status, ORDER_STATUS.OPEN)
        rejected_order = blotter.new_orders[1]
        self.assertEqual(rejected_order.status, ORDER_STATUS.REJECTED)
        self.assertEqual(rejected_order.reason, '')

        # Do it again, but reject it at a later time (after tradesimulation
        # pulls it from new_orders)
        blotter = Blotter(self.sim_params.data_frequency)

        new_open_id = blotter.order(24, 10, MarketOrder())
        new_open_order = blotter.open_orders[24][0]
        self.assertEqual(new_open_id, new_open_order.id)
        # Pretend that the trade simulation did this.
        blotter.new_orders = []

        rejection_reason = "Not enough cash on hand."
        blotter.reject(new_open_id, reason=rejection_reason)
        rejected_order = blotter.new_orders[0]
        self.assertEqual(rejected_order.id, new_open_id)
        self.assertEqual(rejected_order.status, ORDER_STATUS.REJECTED)
        self.assertEqual(rejected_order.reason, rejection_reason)

        # You can't reject a filled order.
        # Reset for paranoia
        blotter = Blotter(self.sim_params.data_frequency)
        blotter.slippage_func = FixedSlippage()
        filled_id = blotter.order(24, 100, MarketOrder())
        filled_order = None
        blotter.current_dt = self.sim_params.trading_days[-1]
        txns, _ = blotter.get_transactions(self.data_portal)
        for txn in txns:
            filled_order = blotter.orders[txn.order_id]

        self.assertEqual(filled_order.id, filled_id)
        self.assertIn(filled_order, blotter.new_orders)
        self.assertEqual(filled_order.status, ORDER_STATUS.FILLED)
        self.assertNotIn(filled_order, blotter.open_orders[24])

        blotter.reject(filled_id)
        updated_order = blotter.orders[filled_id]
        self.assertEqual(updated_order.status, ORDER_STATUS.FILLED)

    def test_order_hold(self):
        """
        Held orders act almost identically to open orders, except for the
        status indication. When a fill happens, the order should switch
        status to OPEN/FILLED as necessary
        """
        blotter = Blotter(self.sim_params.data_frequency)
        # Nothing happens on held of a non-existent order
        blotter.hold(56)
        self.assertEqual(blotter.new_orders, [])

        open_id = blotter.order(24, 100, MarketOrder())
        open_order = blotter.open_orders[24][0]
        self.assertEqual(open_order.id, open_id)

        blotter.hold(open_id)
        self.assertEqual(len(blotter.new_orders), 1)
        self.assertEqual(len(blotter.open_orders[24]), 1)
        held_order = blotter.new_orders[0]
        self.assertEqual(held_order.status, ORDER_STATUS.HELD)
        self.assertEqual(held_order.reason, '')

        blotter.cancel(held_order.id)
        self.assertEqual(len(blotter.new_orders), 1)
        self.assertEqual(len(blotter.open_orders[24]), 0)
        cancelled_order = blotter.new_orders[0]
        self.assertEqual(cancelled_order.id, held_order.id)
        self.assertEqual(cancelled_order.status, ORDER_STATUS.CANCELLED)

        for data in ([100, self.sim_params.trading_days[0]],
                     [400, self.sim_params.trading_days[1]]):
            # Verify that incoming fills will change the order status.
            trade_amt = data[0]
            dt = data[1]

            order_size = 100
            expected_filled = int(trade_amt *
                                  DEFAULT_VOLUME_SLIPPAGE_BAR_LIMIT)
            expected_open = order_size - expected_filled
            expected_status = ORDER_STATUS.OPEN if expected_open else \
                ORDER_STATUS.FILLED

            blotter = Blotter(self.sim_params.data_frequency)
            open_id = blotter.order(24, order_size, MarketOrder())
            open_order = blotter.open_orders[24][0]
            self.assertEqual(open_id, open_order.id)
            blotter.hold(open_id)
            held_order = blotter.new_orders[0]

            filled_order = None
            blotter.current_dt = dt
            txns, _ = blotter.get_transactions(self.data_portal)
            for txn in txns:
                filled_order = blotter.orders[txn.order_id]

            self.assertEqual(filled_order.id, held_order.id)
            self.assertEqual(filled_order.status, expected_status)
            self.assertEqual(filled_order.filled, expected_filled)
            self.assertEqual(filled_order.open_amount, expected_open)