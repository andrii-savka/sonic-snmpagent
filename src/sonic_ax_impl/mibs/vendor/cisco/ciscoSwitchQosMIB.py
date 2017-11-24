from enum import unique, Enum
from bisect import bisect_right

from sonic_ax_impl import mibs
from ax_interface import MIBMeta, ValueType, MIBUpdater, MIBEntry, SubtreeMIBEntry
from ax_interface.encodings import ObjectIdentifier

# Maps SNMP queue stat counters to SAI counters and type
CounterMap = {
    # Unicast send packets
    1: (b'SAI_QUEUE_STAT_PACKETS', b'SAI_QUEUE_TYPE_UNICAST'),
    # Unicast send bytes
    2: (b'SAI_QUEUE_STAT_BYTES', b'SAI_QUEUE_TYPE_UNICAST'),
    # Multicast send packets
    3: (b'SAI_QUEUE_STAT_PACKETS',b'SAI_QUEUE_TYPE_MULTICAST'),
    # Multicast send bytes
    4: (b'SAI_QUEUE_STAT_BYTES',b'SAI_QUEUE_TYPE_MULTICAST'),
    # Unicast dropped packets
    5: (b'SAI_QUEUE_STAT_DROPPED_PACKETS',b'SAI_QUEUE_TYPE_UNICAST'),
    # Unicast dropped bytes
    6: (b'SAI_QUEUE_STAT_DROPPED_BYTES',b'SAI_QUEUE_TYPE_UNICAST'),
    # Multicast dropped packets
    7: (b'SAI_QUEUE_STAT_DROPPED_PACKETS',b'SAI_QUEUE_TYPE_MULTICAST'),
    # Multicast dropped bytes
    8: (b'SAI_QUEUE_STAT_DROPPED_BYTES', b'SAI_QUEUE_TYPE_MULTICAST')
}


class DirectionTypes(int, Enum):
    """
    Queue direction types
    """
    INGRESS = 1
    EGRESS = 2


class QueueStatUpdater(MIBUpdater):
    """
    Class to update the info from Counter DB and to handle the SNMP request
    """
    def __init__(self):
        """
        init the updater
        """
        super().__init__()
        self.db_conn = mibs.init_db()
        self.lag_name_if_name_map = {}
        self.if_name_lag_name_map = {}
        self.oid_lag_name_map = {}
        self.queue_type_map = {}

        self.min_counter = min(CounterMap.keys())
        self.max_counter = max(CounterMap.keys())

        self.reinit_data()

    def reinit_data(self):
        """
        Subclass update interface information
        """
        self.if_name_map, \
        self.if_alias_map, \
        self.if_id_map, \
        self.oid_sai_map, \
        self.oid_name_map = mibs.init_sync_d_interface_tables(self.db_conn)

        self.port_queues_map, self.queue_stat_map, self.port_queue_list_map = \
            mibs.init_sync_d_queue_tables(self.db_conn)

        # Queue index in SNMP OID is 1 based. Convert values from DB.
        for iface, queue_list in self.port_queue_list_map.items():
            self.port_queue_list_map[iface] = list(map(lambda q: q + 1, queue_list))

        self.queue_type_map = self.db_conn.get_all(mibs.COUNTERS_DB, "COUNTERS_QUEUE_TYPE_MAP", blocking=False)

    def update_data(self):
        """
        Update redis (caches config)
        Pulls the table references for each queue.
        """
        for queue_key, sai_id in self.port_queues_map.items():
            queue_stat_name = mibs.queue_table(sai_id)
            queue_stat = self.db_conn.get_all(mibs.COUNTERS_DB, queue_stat_name, blocking=False)
            if queue_stat is not None:
                self.queue_stat_map[queue_stat_name] = queue_stat

        self.lag_name_if_name_map, \
        self.if_name_lag_name_map, \
        self.oid_lag_name_map = mibs.init_sync_d_lag_tables(self.db_conn)

        self.if_range = sorted(list(self.oid_sai_map.keys()) + list(self.oid_lag_name_map.keys()))

    def get_next(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the next sub id.
        """

        if not sub_id:
            return self.if_range[0], int(DirectionTypes.INGRESS), \
                   self.port_queue_list_map[self.if_range[0]][0], self.min_counter

        if len(sub_id) == 1:
            return sub_id + (int(DirectionTypes.INGRESS), self.port_queue_list_map[sub_id[0]][0], self.min_counter)

        if len(sub_id) == 2:
            return sub_id + (self.port_queue_list_map[sub_id[0]][0], self.min_counter)

        if len(sub_id) == 3:
            return sub_id + (self.min_counter, )

        ifindex, direction, queue, counter = sub_id

        if counter < self.max_counter:
            counter += 1
            return ifindex, direction, queue, counter

        counter = self.min_counter

        if queue < max(self.port_queue_list_map[ifindex]):
            queue_idx = self.port_queue_list_map[ifindex].index(queue) + 1
            queue = self.port_queue_list_map[ifindex][queue_idx]
            return ifindex, direction, queue, counter

        if direction == int(DirectionTypes.INGRESS):
            direction = int(DirectionTypes.EGRESS)
            queue = self.port_queue_list_map[ifindex][0]
            return ifindex, direction, queue, counter

        ifindex_idx = self.if_range.index(ifindex)
        if ifindex_idx == len(self.if_range) - 1:
            return None

        ifindex = self.if_range[ifindex_idx + 1]
        direction = int(DirectionTypes.INGRESS)
        queue = self.port_queue_list_map[ifindex][0]
        counter = self.min_counter

        return ifindex, direction, queue, counter

    def _get_counter(self, if_index, queue_index, queue_counter_id):
        """
        :param sub_id: The interface OID.
        :param counter_name: the redis table (either IntEnum or string literal) to query.
        :return: the counter for the respective sub_id/table.
        """
        queue_oid = ''

        try:
            key = mibs.queue_key(if_index, queue_index)
            queue_oid = self.port_queues_map[key]
        except KeyError as e:
            mibs.logger.warning("queue map has no oid for {} port, {} queue.".format(if_index, queue_index))
            return None

        queue_stat_table_name = mibs.queue_table(queue_oid)
        queue_type = ''

        queue_type = self.queue_type_map.get(queue_oid)
        if not queue_type:
            mibs.logger.warning("unable to get the queue type for {} queue of {} port.".format(queue_index, if_index))
            return None

        try:
            counter_sai_name, counter_sai_type = CounterMap[queue_counter_id]
        except KeyError as e:
            mibs.logger.warning("unable to map the sai counter for {} counter of {} queue. {} port.".format(queue_counter_id, queue_index, if_index))
            return None

        # queue has different type then requested counter
        if queue_type != counter_sai_type:
            return 0

        counter_value = ''

        try:
            counter_value = self.queue_stat_map[queue_stat_table_name][counter_sai_name]
        except KeyError as e:
            mibs.logger.warning("queue stat map has no {} table or {} counter in described table.".format(queue_stat_table_name, counter_sai_name))
            return 0

        counter_value = int(counter_value)

        return counter_value

    def handle_stat_request(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the counter for the respective sub_id/table.
        """
        # if_index, if_direction, queue_index and counter id should be passed

        if len(sub_id) != 4:
            return

        if_index = sub_id[0]
        if_direction = sub_id[1]
        queue_index = int(sub_id[2]) - 1
        queue_counter_id = sub_id[3]

        # Currently, Sonic supports only egress queues
        if if_direction != DirectionTypes.EGRESS:
            return 0

        if if_index in self.oid_lag_name_map:
            counter_value = 0
            for lag_member in self.lag_name_if_name_map[self.oid_lag_name_map[if_index]]:
                counter_value += self._get_counter(mibs.get_index(lag_member), queue_index, queue_counter_id)

            return counter_value
        else:
            return self._get_counter(if_index, queue_index, queue_counter_id)


class csqIfQosGroupStatsTable(metaclass=MIBMeta, prefix='.1.3.6.1.4.1.9.9.580.1.5.5'):
    """
    'csqIfQosGroupStatsTable' http://oidref.com/1.3.6.1.4.1.9.9.580.1.5.5
    """

    queue_updater = QueueStatUpdater()

    # csqIfQosGroupStatsTable = '1.3.6.1.4.1.9.9.580.1.5.5'
    # csqIfQosGroupStatsEntry = '1.3.6.1.4.1.9.9.580.1.5.5.1.4'

    queue_stat_request = \
        SubtreeMIBEntry('1.4', queue_updater, ValueType.INTEGER, queue_updater.handle_stat_request)
