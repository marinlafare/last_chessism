"""Offline process budgeting, failure and cancellation contracts."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
from chessism_api.operations import tablebase_parallel as p


class BudgetTests(unittest.TestCase):
    def test_quota_affinity_and_hard_limit(self):
        for quota,requested,expected in [('100000 100000',4,1),('250000 100000',4,2),
                ('max 100000',8,4),('50000 100000',2,1)]:
            with patch.object(p.Path,'read_text',return_value=quota),patch.object(p.os,'sched_getaffinity',return_value=set(range(8))):
                self.assertEqual(p.worker_count(requested),expected)
        with patch.object(p.Path,'read_text',return_value='max 100000'),patch.object(p.os,'sched_getaffinity',return_value={0}):
            self.assertEqual(p.worker_count(4),1)


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_pool_is_reused_and_output_order_is_preserved(self):
        pool=MagicMock()
        pool.map_async.side_effect=lambda fn,parts:SimpleNamespace(ready=lambda:True,
            get=lambda:[([r for r in part if r['valid']],[r for r in part if not r['valid']]) for part in parts])
        rows=[{'id':i,'valid':i%3!=0} for i in range(20)]
        context=MagicMock();context.Pool.return_value=pool
        with patch.object(p,'worker_count',return_value=2),patch.object(p.multiprocessing,'get_context',return_value=context):
            async with p.TablebaseProber('/synthetic') as prober:
                for _ in range(2):
                    solved,missing=await prober.probe(rows)
                    self.assertEqual(solved,[r for r in rows if r['valid']])
                    self.assertEqual(missing,[r for r in rows if not r['valid']])
                context.Pool.assert_called_once()
        pool.close.assert_called_once();pool.join.assert_called_once();pool.terminate.assert_not_called()

    async def test_timeout_and_cancellation_terminate_and_join_children(self):
        for cancel in (False,True):
            pool=MagicMock();pool.map_async.return_value.ready.return_value=False
            with patch.object(p,'worker_count',return_value=2):
                prober=p.TablebaseProber('/synthetic',timeout=.03);prober.pool=pool
                task=asyncio.create_task(prober.probe([{'id':1}]))
                if cancel:
                    await asyncio.sleep(.01);task.cancel()
                with self.assertRaises(asyncio.CancelledError if cancel else asyncio.TimeoutError):await task
            pool.terminate.assert_called_once();pool.join.assert_called_once()
            self.assertIsNone(prober.pool)

    async def test_worker_failure_is_not_reported_as_missing_table(self):
        pool=MagicMock();pool.map_async.return_value.ready.return_value=True
        pool.map_async.return_value.get.side_effect=RuntimeError('worker broke')
        with patch.object(p,'worker_count',return_value=2):
            prober=p.TablebaseProber('/synthetic');prober.pool=pool
            with self.assertRaisesRegex(RuntimeError,'worker broke'):await prober.probe([{}])
        pool.terminate.assert_called_once();pool.join.assert_called_once()


if __name__=='__main__':unittest.main()
