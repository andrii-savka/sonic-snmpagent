import imp
import re
import sys

from sonic_ax_impl import mibs
from ax_interface import MIBMeta, ValueType, MIBUpdater, MIBEntry, SubtreeMIBEntry
from ax_interface.encodings import ObjectIdentifier

PSU_PLUGIN_MODULE_NAME = 'psuutil'
PSU_PLUGIN_MODULE_PATH = "/usr/share/sonic/platform/plugins/{}.py".format(PSU_PLUGIN_MODULE_NAME)
PSU_PLUGIN_CLASS_NAME = 'PsuUtil'

class PowerStatusHandler:
    """
    Class to handle the SNMP request
    """
    def __init__(self):
        """
        init the handler
        """
        self.psuutil = None

        try:
            module = imp.load_source(PSU_PLUGIN_MODULE_NAME, PSU_PLUGIN_MODULE_PATH)
        except ImportError as e:
            mibs.logger.error("Failed to load PSU module '%s': %s" % (PSU_PLUGIN_MODULE_NAME, str(e)), True)
            sys.exit()

        try:
            platform_psuutil_class = getattr(module, PSU_PLUGIN_CLASS_NAME)
            self.psuutil = platform_psuutil_class()
        except AttributeError as e:
            mibs.logger.error("Failed to instantiate '%s' class: %s" % (PLATFORM_SPECIFIC_CLASS_NAME, str(e)), True)
            sys.exit()

    def _getPsuIndex(self, sub_id):
        """
        Get the PSU index from sub_id
        :return: the index of supported PSU
        """
        if not sub_id or len(sub_id) > 1:
            return None

        psu_index = int(sub_id[0])

        if psu_index < 1 or psu_index > self.psuutil.get_num_psus():
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

        if psu_index and psu_index + 1 <= self.psuutil.get_num_psus():
            return (psu_index + 1,)

        return None

    def getPsuStatus(self, sub_id):
        """
        :param sub_id: The 1-based sub-identifier query.
        :return: the status of requested PSU
                 1 - PSU has correct functionalling
                 0 - PSU has a problem with functionalling
        """
        psu_index = self._getPsuIndex(sub_id)

        if not psu_index:
            return None

        psu_status = self.psuutil.get_psu_status(psu_index)

        if psu_status:
            return 1

        return 0


class cefcFruPowerStatusTable(metaclass=MIBMeta, prefix='.1.3.6.1.4.1.9.9.117.1.1.2'):
    """
    'cefcFruPowerStatusTable' http://oidref.com/1.3.6.1.4.1.9.9.117.1.1.2
    """

    power_status_handler = PowerStatusHandler()

    # cefcFruPowerStatusTable = '1.3.6.1.4.1.9.9.117.1.1.2'
    # csqIfQosGroupStatsEntry = '1.3.6.1.4.1.9.9.117.1.1.2.1'

    psu_status = SubtreeMIBEntry('1.2', power_status_handler, ValueType.INTEGER, power_status_handler.getPsuStatus)
