"""xApp SDK, modelled on the python framework of srsRAN's oran-sc-ric.

Subclass :class:`xAppBase`, decorate the entry point with
``@xAppBase.start_function`` and use ``self.e2sm_kpm`` / ``self.e2sm_rc``.
The same xApp runs standalone (a separate process connected to the RIC's xApp
interface) or inside the RIC (installed package exposing a ``uric.xapps``
entry point, or ``uric --xapp mypkg.module:MyXapp``).
"""

from ..codec.rc import Key
from ..core.ric import RICError
from .xappbase import SubscriptionWrapper, e2sm_kpm_module, e2sm_rc_module, e2sm_types, xAppBase

__all__ = ["xAppBase", "e2sm_types", "e2sm_kpm_module", "e2sm_rc_module", "SubscriptionWrapper",
           "RICError", "Key"]
