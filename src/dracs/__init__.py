#!/usr/bin/env python3

<<<<<<< Updated upstream
import argparse
import asyncio
import json
import logging
import os
import re
import requests
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Tuple, Optional
from pysnmp.hlapi.v1arch.asyncio import (
    SnmpDispatcher,
    CommunityData,
    UdpTransportTarget,
    ObjectIdentity,
    ObjectType,
    get_cmd,
)
from tabulate import tabulate

__version__ = "1.0.0"
=======
__version__ = "0.1.0"
>>>>>>> Stashed changes

from dracs.exceptions import (  # noqa: F401
    APIError,
    DatabaseError,
    DracsError,
    SNMPError,
    ValidationError,
)
from dracs.validation import (  # noqa: F401
    read_host_list,
    validate_hostname,
    validate_service_tag,
    validate_version,
)
from dracs.db import (  # noqa: F401
    db_initialize,
    get_db_connection,
    query_by_hostname,
    query_by_service_tag,
    upsert_system,
)
from dracs.snmp import build_idrac_hostname, get_snmp_value  # noqa: F401
from dracs.api import dell_api_warranty_date  # noqa: F401
from dracs.commands import (  # noqa: F401
    add_dell_warranty,
    discover_dell_system,
    discover_dell_systems_batch,
    edit_dell_warranty,
    filter_list_results,
    list_dell_warranty,
    lookup_dell_warranty,
    refresh_dell_warranty,
    remove_dell_warranty,
)
from dracs.cli import CustomParser, main, main_cli, setup_logging  # noqa: F401
