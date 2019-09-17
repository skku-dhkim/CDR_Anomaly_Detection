"""
@ File name: rrcf_cls.py
@ Version: 1.0
@ Last update: 2019.Sep.17
@ Author: DH.KIM
@ Company: Ntels Co., Ltd
"""
import rrcf
import timeit
from inspect import getframeinfo, stack


def _debug_info(message, m_type='INFO'):
    insp = getframeinfo(stack()[1][0])
    if m_type == "INFO":
        print("[*] INFO:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "WARNING":
        print("[@] WARNING:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    elif m_type == "ERROR":
        print("[!] ERROR:\n"
              "\t - File: {}:{}\n"
              "\t - Message: {}".format(insp.filename, insp.lineno, message))
    else:
        raise ValueError("Invalid input vlaue \'m_type\': {}".format(m_type))


class RRCF(object):
    def __init__(self, num_trees, sequences, leaves_size):
        self.num_trees = num_trees
        self.sequences = sequences
        self.leaves_size = leaves_size
        self.last_index = None
        self.forest = None
        self.q_index_keys = Queue(size=self.leaves_size)

    def train_rrcf(self, date_time, data, timer=True):
        """[Debug] Need to be tested"""
        if self.forest is not None:
            flag = input("[@] Warning:\n"
                         "\tForest is already exist. Do you want to override? y/[n]: ") or 'n'
            if flag.lower() != 'y':
                return None

        # NOTE: Timer for function execution time.
        train_start = timeit.default_timer()

        self.forest = []
        # NOTE: Build a forest.
        for _ in range(self.num_trees):
            tree = rrcf.RCTree()
            self.forest.append(tree)

        # NOTE: Build a sequences points.
        points = rrcf.shingle(data, size=self.sequences)

        # NOTE: Initialize the average of Collusive Displacement(CoDisp).
        avg_codisp = {}

        for index, point in enumerate(points):
            # NOTE: For each tree in the forest...
            for tree in self.forest:
                # NOTE: If tree is above permitted size, drop the oldest point (FIFO)
                if len(tree.leaves) >= self.leaves_size:
                    tree.forget_point(index - self.leaves_size)
                # NOTE: Insert the new point into the tree
                tree.insert_point(point, index=index)

                # NOTE: Compute CoDisp on the new point and take the average among all trees
                if not date_time[index+self.sequences-1] in avg_codisp:
                    avg_codisp[date_time[index+self.sequences-1]] = 0
                avg_codisp[date_time[index+self.sequences-1]] += tree.codisp(index) / self.num_trees
            self.last_index = index

        # NOTE: Timer for function execution time.
        train_end = timeit.default_timer()

        if timer:
            return avg_codisp, train_end-train_start
        else:
            return avg_codisp

    def anomaly_score(self, date, data, with_date=True):
        if self.forest is None:
            _debug_info("You SHOULD train the forest first. Program exit..", m_type="ERROR")
            raise SystemExit()

        avg_codisp = 0
        insert_index = -1

        # NOTE: Initialize Old queue once before scoring starts.
        if self.q_index_keys.empty():
            if self.last_index >= self.leaves_size:
                l_index_keys = [k+(self.last_index-self.leaves_size+1) for k in range(self.leaves_size)]
            else:
                l_index_keys = [k for k in range(self.last_index+1)]
            self.q_index_keys.migrate(l_index_keys)

        # NOTE: Get index
        if self.q_index_keys.full():
            index = self.q_index_keys.get()
        else:
            self.last_index += 1
            index = self.last_index

        # NOTE: Adding a node to the tree
        for tree in self.forest:
            if len(tree.leaves) >= self.leaves_size:
                tree.forget_point(index)

            insert_index = index % self.leaves_size
            tree.insert_point(data, index=insert_index)

            avg_codisp += tree.codisp(insert_index) / self.num_trees

        if insert_index <= -1:
            _debug_info("Invalid \'insert_index\' value. We have \'{}\'".format(-1), m_type="ERROR")
            raise SystemExit()

        # NOTE: Inserting new index number
        self.q_index_keys.put(insert_index)

        if with_date is True:
            return [date[-1], avg_codisp]
        else:
            return avg_codisp


class Queue(object):
    def __init__(self, size=0):
        self.size = size
        self.l_index = []

    def put(self, index):
        if self.size != 0:
            if len(self.l_index) + 1 > self.size:
                _debug_info("Buffer overflow. Queue should not exceed the size.", m_type="ERROR")
                raise SystemExit()
        self.l_index.append(index)

    def get(self):
        if len(self.l_index) == 0:
            _debug_info("Queue is empty", m_type="ERROR")
            raise SystemExit()
        value = self.l_index[0]
        self.l_index.remove(value)
        return value

    def empty(self):
        if len(self.l_index) != 0:
            return False
        else:
            return True

    def full(self):
        if self.size == 0:
            _debug_info("Queue object has no buffer limit. The \'full\' method is useless in this case.",
                        m_type="WARNING")
            return None
        else:
            if len(self.l_index) >= self.size:
                return True
            else:
                return False

    def migrate(self, prev_index):
        self.l_index = prev_index
