from enum import unique, Enum
from bisect import bisect_right

from sonic_ax_impl import mibs
from ax_interface import MIBMeta, ValueType, MIBUpdater, MIBEntry, SubtreeMIBEntry
from ax_interface.encodings import ObjectIdentifier

class PfcUpdater(MIBUpdater):

    def reinit_data(self):
        """
        Subclass update interface information
        """
        self.if_name_map, \
        self.if_alias_map, \
        self.if_id_map, \
        self.oid_sai_map, \
        self.oid_name_map = mibs.init_sync_d_interface_tables(self.db_conn)

    def update_data(self):
        """
        Update redis (caches config)
        Pulls the table references for each interface.
        """
        self.if_counters = \
            {sai_id: self.db_conn.get_all(mibs.COUNTERS_DB, mibs.counter_table(sai_id), blocking=True)
             for sai_id in self.if_id_map}

        self.lag_name_if_name_map, \
        self.if_name_lag_name_map, \
        self.oid_lag_name_map = mibs.init_sync_d_lag_tables(self.db_conn)

        self.if_range = sorted(list(self.oid_sai_map.keys()) + list(self.oid_lag_name_map.keys()))
        self.if_range = [(i,) for i in self.if_range]

    def __init__(self):
        super().__init__()
        self.db_conn = mibs.init_db()

        self.lag_name_if_name_map = {}
        self.if_name_lag_name_map = {}
        self.oid_lag_name_map = {}

        # cache of interface counters
        self.if_counters = {}
        self.if_range = []

        # init data from Counter DB.
        self.reinit_data()

    def get_next(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the next sub id.
        """
        try:
            if sub_id is None:
                return self.if_range[0]

            right = bisect_right(self.if_range, sub_id)
            if right >= len(self.if_range):
                return None
            return self.if_range[right]
        except (IndexError, KeyError) as e:
            mibs.logger.error("failed to get next oid with error = {}".format(str(e)))

    def get_oid(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the interface OID.
        """
        if sub_id is None or sub_id not in self.if_range:
            return None

        return sub_id[0]

    def _get_counter(self, oid, counter_name):
        """
        :param sub_id: The interface OID.
        :param counter_name: the redis table (either IntEnum or string literal) to query.
        :return: the counter for the respective sub_id/table.
        """
        sai_id = self.oid_sai_map[oid]

        # Enum.name or counter_name = 'name_of_the_table'
        _counter_name = bytes(getattr(counter_name, 'name', counter_name), 'utf-8')

        try:
            counter_value = self.if_counters[sai_id][_counter_name]
            counter_value = int(counter_value)
            # done!
            return counter_value
        except KeyError as e:
            mibs.logger.warning("SyncD 'COUNTERS_DB' missing attribute '{}'.".format(e))
            return None

    def cpfcIfRequests(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the counter for the respective sub_id/table.
        """
        oid = self.get_oid(sub_id)
        if oid is None:
            return None

        counter_name = 'SAI_PORT_STAT_PFC_3_RX_PKTS'

        if oid in self.oid_lag_name_map:
            counter_value = 0
            for lag_member in self.lag_name_if_name_map[self.oid_lag_name_map[oid]]:
                counter_value += self._get_counter(mibs.get_index(lag_member), counter_name)

            return counter_value
        else:
            return self._get_counter(oid, counter_name)


    def cpfcIfIndications(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the counter for the respective sub_id/table.
        """
        oid = self.get_oid(sub_id)
        if oid is None:
            return None

        counter_name = 'SAI_PORT_STAT_PFC_3_TX_PKTS'

        if oid in self.oid_lag_name_map:
            counter_value = 0
            for lag_member in self.lag_name_if_name_map[self.oid_lag_name_map[oid]]:
                counter_value += self._get_counter(mibs.get_index(lag_member), counter_name)

            return counter_value
        else:
            return self._get_counter(oid, counter_name)


class PfcPrioUpdater(PfcUpdater):
    def __init__(self):
        super().__init__()
        self.max_prio = 8

    def queue_index(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the 0-based interface ID.
        """
        if len(sub_id) >= 2:
            return sub_id[1] - 1
        return None

    def get_next(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the next sub id.
        """
        try:
            if sub_id is None:
               return (self.if_range[0][0], 1)

            if len(sub_id) < 2:
               return (sub_id[0], 1)

            if sub_id[1] >= self.max_prio:
                return None

            right = sub_id[1] + 1

            return (sub_id[0], right)
        except (IndexError, KeyError) as e:
            mibs.logger.error("failed to get next oid with error = {}".format(str(e)))

    def requestsPerPriority(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the counter for the respective sub_id/table.
        """        
        port_oid = ''
        queue_index = ''
        try:
            port_oid = self.get_oid((sub_id[0],))
            queue_index = self.queue_index(sub_id)
            if port_oid is None or queue_index is None:
                return None
        except (IndexError, KeyError):
            mibs.logger.warning("requestsPerPriority: incorrect sub_id = {}".format(str(sub_id)))
            return None

        counter_name = 'SAI_PORT_STAT_PFC_' + str(queue_index) + '_RX_PKTS'

        if port_oid in self.oid_lag_name_map:
            counter_value = 0
            for lag_member in self.lag_name_if_name_map[self.oid_lag_name_map[port_oid]]:
                counter_value += self._get_counter(mibs.get_index(lag_member), counter_name)

            return counter_value
        else:
            return self._get_counter(port_oid, counter_name)

    def indicationsPerPriority(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the counter for the respective sub_id/table.
        """
        port_oid = ''
        queue_index = ''
        try:
            port_oid = self.get_oid((sub_id[0],))
            queue_index = self.queue_index(sub_id)
            if port_oid is None or queue_index is None:
                return None
        except (IndexError, KeyError):
            mibs.logger.warning("indicationsPerPriority: incorrect sub_id = {}".format(str(sub_id)))
            return None

        counter_name = 'SAI_PORT_STAT_PFC_' + str(queue_index) + '_TX_PKTS'

        if port_oid in self.oid_lag_name_map:
            counter_value = 0
            for lag_member in self.lag_name_if_name_map[self.oid_lag_name_map[port_oid]]:
                counter_value += self._get_counter(mibs.get_index(lag_member), counter_name)

            return counter_value
        else:
            return self._get_counter(port_oid, counter_name)


# cpfcIfTable = '1.1'
# cpfcIfEntry = '1.1.1.x'
class cpfcIfTable(metaclass=MIBMeta, prefix='.1.3.6.1.4.1.9.9.813.1.1'):
    """
    'ciscoPfcExtMIB' http://oidref.com/1.3.6.1.4.1.9.9.813.1.1
    """
    pfc_updater = PfcUpdater()

    ifRequests = \
        SubtreeMIBEntry('1.1', pfc_updater, ValueType.INTEGER, pfc_updater.cpfcIfRequests)

    ifIndications = \
        SubtreeMIBEntry('1.2', pfc_updater, ValueType.INTEGER, pfc_updater.cpfcIfIndications)


# cpfcIfPriorityTable = '1.2'
# cpfcIfPriorityEntry = '1.2.x'
class cpfcIfPriorityTable(metaclass=MIBMeta, prefix='.1.3.6.1.4.1.9.9.813.1.2'):
    """
    'ciscoPfcExtMIB' http://oidref.com/1.3.6.1.4.1.9.9.813
    """
    pfc_updater = PfcPrioUpdater()

    prioRequests = \
        SubtreeMIBEntry('1.2', pfc_updater, ValueType.INTEGER, pfc_updater.requestsPerPriority)

    prioIndications = \
        SubtreeMIBEntry('1.3', pfc_updater, ValueType.INTEGER, pfc_updater.indicationsPerPriority)