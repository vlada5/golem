import datetime
import random
import sys
import time

from golem.model import db
from golem.model import ExpectedIncome
from golem.model import Income
from golem.network.p2p.node import Node
from golem.testutils import PEP8MixIn
from golem.tools.testwithdatabase import TestWithDatabase
from golem.transactions.incomeskeeper import IncomesKeeper

# to ensure that Golem's wrapper for sql BigIntegerField does not overflow
BIG_INT = 2 ** 63 - 1 # (9,223,372,036,854,775,807)

def generate_some_id(prefix='test'):
    return "%s-%d-%d" % (prefix, time.time() * 1000, random.random() * 1000)


class TestIncomesKeeper(TestWithDatabase, PEP8MixIn):
    PEP8_FILES = [
        'golem/transactions/incomeskeeper.py',
    ]

    def setUp(self):
        super(TestIncomesKeeper, self).setUp()
        random.seed()
        self.incomes_keeper = IncomesKeeper()

    def _test_expect_income(self, sender_node_id, task_id, subtask_id, value):
        self.incomes_keeper.expect(
            sender_node_id=sender_node_id,
            task_id=task_id,
            subtask_id=subtask_id,
            p2p_node=Node(),
            value=value
        )
        with db.atomic():
            expected_income = ExpectedIncome.get(sender_node=sender_node_id, task=task_id, subtask=subtask_id)
        self.assertEqual(expected_income.value, value)

    def test_received(self):
        sender_node_id = generate_some_id('sender_node_id')
        task_id = generate_some_id('task_id')
        subtask_id = generate_some_id('subtask_id')
        value = random.randint(BIG_INT+1, BIG_INT+10)

        self.assertEqual(ExpectedIncome.select().count(), 0)
        self._test_expect_income(sender_node_id=sender_node_id,
                                 task_id=task_id,
                                 subtask_id=subtask_id,
                                 value=value
                                 )
        self.assertEqual(ExpectedIncome.select().count(), 1)

        transaction_id = generate_some_id('transaction_id')
        block_number = random.randint(0, sys.maxsize)
        income = self.incomes_keeper.received(
            sender_node_id=sender_node_id,
            task_id=task_id,
            subtask_id=subtask_id,
            transaction_id=transaction_id,
            block_number=block_number,
            value=value
        )
        self.assertEqual(ExpectedIncome.select().count(), 0)
        self.assertIsNotNone(income)

        with db.atomic():
            income = Income.get(sender_node=sender_node_id, task=task_id, subtask=subtask_id)
        self.assertEqual(income.value, value)
        self.assertEqual(income.transaction, transaction_id)
        self.assertEqual(income.block_number, block_number)

        # try to duplicate db key - same sender cannot pay for the same subtask twice ;p
        new_transaction = generate_some_id('transaction_id2')
        new_value = random.randint(BIG_INT+1, BIG_INT+10)
        income = self.incomes_keeper.received(
            sender_node_id=sender_node_id,
            task_id=task_id,
            subtask_id=subtask_id,
            transaction_id=new_transaction,
            block_number=block_number,
            value=new_value
        )
        self.assertIsNone(income)

    def test_run_once(self):
        sender_node_id = generate_some_id('sender_node_id')
        task_id = generate_some_id('task_id')
        subtask_id = generate_some_id('subtask_id')
        value = random.randint(BIG_INT+1, BIG_INT+10)
        transaction_id = generate_some_id('transaction_id')

        expected_income = self.incomes_keeper.expect(
            sender_node_id=sender_node_id,
            p2p_node=Node(),
            task_id=task_id,
            subtask_id=subtask_id,
            value=value
        )


        # expected payment written to DB
        self.assertEqual(ExpectedIncome.select().count(), 1)


        # Time is right but no matching payment received
        with db.atomic():
            expected_income.modified_date = datetime.datetime.now() - datetime.timedelta(hours=1)
            expected_income.save()

        self.incomes_keeper.run_once()
        self.assertEqual(ExpectedIncome.select().count(), 1)


        # Matching received but too early to check
        income = Income.create(
            sender_node=sender_node_id,
            task=task_id,
            subtask=subtask_id,
            transaction=transaction_id,
            block_number=random.randint(0, sys.maxsize),
            value=value)

        with db.atomic():
            self.assertEqual(ExpectedIncome.select().count(), 1)
            expected_income.modified_date = datetime.datetime.now() + datetime.timedelta(hours=1)
            expected_income.save()

        self.incomes_keeper.run_once()
        self.assertEqual(ExpectedIncome.select().count(), 1)


        # Match
        with db.atomic():
            expected_income.modified_date = datetime.datetime.now() - datetime.timedelta(hours=1)
            expected_income.save()

        self.incomes_keeper.run_once()
        with db.atomic():
            self.assertEqual(ExpectedIncome.select().count(), 0)


    def test_wtf (self):
        super(TestIncomesKeeper, self).setUp()

        import mock
        import random
        import sys
        import uuid

        from golem.model import db
        from golem import model
        from golem import testutils
        from golem.transactions.ethereum.ethereumincomeskeeper \
            import EthereumIncomesKeeper

        random.seed()

        def get_some_id():
            return str(uuid.uuid4())

        def get_receiver_id():
            return '0x0000000000000000000000007d577a597b2742b498cb5cf0c26cdcd726d39e6e'

        processor = mock.MagicMock()
        processor.eth_address.return_value = get_receiver_id()
        processor.synchronized.return_value = True
        self.instance = EthereumIncomesKeeper(processor)

        SQLITE3_MAX_INT = 2 ** 31 - 1

        received_kwargs = {
            'sender_node_id': get_some_id(),
            'task_id': get_some_id(),
            'subtask_id': 's1' + get_some_id()[:-2],
            'transaction_id': get_some_id(),
            'block_number': random.randint(0, int(SQLITE3_MAX_INT / 2)),
            'value': SQLITE3_MAX_INT - 1,
        }

        self.instance.processor.get_logs.return_value = [
            {
                'topics': [
                    EthereumIncomesKeeper.LOG_ID,
                    get_some_id(),  # sender
                    self.instance.processor.eth_address(),  # receiver
                ],
                'data': hex(received_kwargs['value']),
            },
        ]

        inco = self.instance.received(**received_kwargs)

        from golem.model import Income
        # with db.atomic():
        #     getincome = Income.get(sender_node=received_kwargs['sender_node_id'], task=received_kwargs['task_id'], subtask=received_kwargs['subtask_id'])
        #     x =3

        income = model.Income.select().where(
            model.Income.subtask == received_kwargs['subtask_id'])


        with db.atomic():
            getincome = Income.get(sender_node=received_kwargs['sender_node_id'], task=received_kwargs['task_id'], subtask=received_kwargs['subtask_id'])
            x=4
        x=4