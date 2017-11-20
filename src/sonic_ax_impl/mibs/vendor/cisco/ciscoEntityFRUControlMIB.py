import subprocess
import re

from sonic_ax_impl import mibs
from ax_interface import MIBMeta, ValueType, MIBUpdater, MIBEntry, SubtreeMIBEntry
from ax_interface.encodings import ObjectIdentifier

import pdb

class PowerStatusHandler:
    """
    Class to handle the SNMP request
    """
    def __init__(self):
        """
        init the handler
        """

        # Holds the number of supported PSU
        self.num_psus = self._getNumOfSupportedPsu()

    def _getNumOfSupportedPsu(self):
        """
        :return: the number of supported PSU
        """
        try:
            output = subprocess.check_output(['psuutil', 'numpsus'])
            if str(output[0]).isdigit():
                return int(output)
        except subprocess.CalledProcessError:
            mibs.logger.error("failed to run psuutil tool")
        except IndexError:
            mibs.logger.error("failed to get he number of supported PSU")

        return 0

    def _getPsuIndex(self, sub_id):
        """
        Get the PSU index from sub_id
        :return: the index of supported PSU
        """
        if not sub_id or len(sub_id) > 1:
            return None

        psu_index = int(sub_id[0])

        if psu_index < 1 or psu_index > self.num_psus:
            return None

        return psu_index

    def get_next(self, sub_id):
        """
        :param sub_id: The 1-based snmp sub-identifier query.
        :return: the next sub id.
        """
        if not sub_id:
            return (1,)

        psu_index = self._getPsuIndex(sub_id)

        if psu_index and psu_index + 1 <= self.num_psus:
            return (psu_index + 1,)

        return None

    def getPsuStatus(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the status of requested PSU.
        """
        psu_index = self._getPsuIndex(sub_id)

        if not psu_index:
            return None

        output = ""
        try:
            output = subprocess.check_output(['psuutil', 'status', '-i', str(psu_index), '--textonly'])
        except subprocess.CalledProcessError:
            mibs.logger.error("failed to get PSU status from psuutil tool")
            return None

        psu_status = 0
        status_list = str(output).split(':')

        try:
            if re.match('^OK.*', status_list[1]) and str(psu_index) in status_list[0]:
                psu_status = 1
        except IndexError:
            mibs.logger.error("failed to get psu status, output = {}".format(output))

        return psu_status


class cefcFRUPowerStatusTable(metaclass=MIBMeta, prefix='.1.3.6.1.4.1.9.9.117.1.1.2'):
    """
    'cefcFRUPowerStatusTable' http://oidref.com/1.3.6.1.4.1.9.9.117.1.1.2
    """

    power_status_handler = PowerStatusHandler()

    # cefcFRUPowerStatusTable = '1.3.6.1.4.1.9.9.117.1.1.2'
    # csqIfQosGroupStatsEntry = '1.3.6.1.4.1.9.9.117.1.1.2.1'

    psu_status = SubtreeMIBEntry('1.2', power_status_handler, ValueType.INTEGER, power_status_handler.getPsuStatus)
