import asyncio
import json
import logging
import os
import sqlite3
import time
from typing import List, Tuple, Optional

from tabulate import tabulate

from dracs.exceptions import (
    DatabaseError,
    DracsError,
    SNMPError,
    ValidationError,
)
from dracs.db import (
    get_db_connection,
    db_initialize,
    query_by_service_tag,
    query_by_hostname,
    upsert_system,
)
from dracs.snmp import get_snmp_value, build_idrac_hostname
from dracs.api import dell_api_warranty_date

logger = logging.getLogger(__name__)

debug_output = False


async def add_dell_warranty(
    service_tag: str, hostname: str, model: str, warranty: str
) -> None:
    """
    Logic for the 'add' command. Fetches hardware versions via SNMP and
    warranty dates via API, then saves the new record to the local DB.
    """
    idrac_host = build_idrac_hostname(hostname)
    community_string = os.getenv("SNMP_COMMUNITY", "public")
    BIOS_OID = "1.3.6.1.4.1.674.10892.5.4.300.50.1.8.1.1"
    IDRAC_FW_OID = "1.3.6.1.4.1.674.10892.5.1.1.8.0"

    bios_version = await get_snmp_value(idrac_host, community_string, BIOS_OID)
    idrac_version = await get_snmp_value(idrac_host, community_string, IDRAC_FW_OID)

    logger.info(f"Retrieved SNMP values - BIOS: {bios_version}, iDRAC: {idrac_version}")

    db_initialize(warranty)

    with get_db_connection(warranty) as conn:
        cursor = conn.cursor()
        query = """
            SELECT * FROM systems
            WHERE svc_tag = :service_tag
               AND name = :hostname
        """
        params = {"service_tag": service_tag, "hostname": hostname}
        cursor.execute(query, params)
        results = cursor.fetchall()
    if debug_output:
        logger.debug(f"service_tag = {service_tag}")
        logger.debug(f"hostname = {hostname}")
        logger.debug(f"warranty = {warranty}")
        logger.debug(f"query = {query}")
        logger.debug(f"params = {params}")
        logger.debug(f"results = {results}")

    if len(results) > 1:
        raise DatabaseError("Multiple matching records found in database")

    # If the host is already in the DB, then we
    # update the FW and BIOS versions, as well as model.
    # No need to reach out to Dell to refetch warranty
    if len(results) == 1:
        logger.info(f"Updating existing record for {service_tag}")
        exp_date = results[0][5]
        exp_epoch = results[0][6]
        with get_db_connection(warranty) as conn:
            cursor = conn.cursor()
            data = {
                "svc_tag": service_tag,
                "name": hostname,
                "model": model,
                "idrac_version": idrac_version,
                "bios_version": bios_version,
                "exp_date": exp_date,
                "exp_epoch": exp_epoch,
            }
            cursor.execute(
                """
                INSERT OR REPLACE INTO systems
                VALUES (:svc_tag, :name, :model,
                :idrac_version, :bios_version,
                :exp_date, :exp_epoch)
            """,
                data,
            )
            conn.commit()
        logger.info(f"Successfully updated record for {service_tag}")
    else:
        # get warranty from Dell API
        logger.info(
            f"Adding new record for {service_tag}, fetching warranty from Dell API"
        )
        h_epoch, h_date = dell_api_warranty_date(service_tag)
        result = {"svctag": service_tag}
        result["exp_date"] = h_date
        result["exp_epoch"] = h_epoch
        result["hostname"] = hostname
        result["model"] = model
        result["bios_version"] = bios_version
        result["idrac_version"] = idrac_version

        if debug_output:
            logger.debug(f"Warranty result: {result}")

        with get_db_connection(warranty) as conn:
            cursor = conn.cursor()
            data = {
                "svc_tag": service_tag,
                "name": hostname,
                "model": model,
                "idrac_version": idrac_version,
                "bios_version": bios_version,
                "exp_date": result["exp_date"],
                "exp_epoch": result["exp_epoch"],
            }
            logger.debug(f"Inserting data: {data}")
            cursor.execute(
                """
                INSERT OR REPLACE INTO systems
                VALUES (:svc_tag, :name, :model,
                :idrac_version, :bios_version,
                :exp_date, :exp_epoch)
            """,
                data,
            )
            conn.commit()
        logger.info(f"Successfully added record for {service_tag}")


async def edit_dell_warranty(
    service_tag: Optional[str],
    hostname: Optional[str],
    model: Optional[str],
    idrac: bool,
    bios: bool,
    warranty: str,
) -> None:
    """
    Logic for the 'edit' command. Allows updating specific fields (model, BIOS, iDRAC)
    for an existing record in the database without re-fetching warranty dates.
    """
    if service_tag:
        if debug_output:
            print(f"service_tag = {service_tag}")
    if hostname:
        if debug_output:
            print(f"hostname = {hostname}")
    if model:
        if debug_output:
            logger.debug(f"model = {model}")
    else:
        if not idrac and not bios:
            raise ValidationError(
                "Model parameter required for edit mode when not updating idrac or bios"
            )

    db_initialize(warranty)
    conn = sqlite3.connect(warranty)
    cursor = conn.cursor()
    if service_tag:
        query = """
            SELECT * FROM systems
            WHERE svc_tag = :service_tag
        """
        params = {"service_tag": service_tag}
    if hostname:
        query = """
            SELECT * FROM systems
            WHERE name = :hostname
        """
        params = {"hostname": hostname}
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()

    if debug_output:
        logger.debug(f"service_tag = {service_tag}")
        logger.debug(f"hostname = {hostname}")
        logger.debug(f"warranty = {warranty}")
        logger.debug(f"query = {query}")
        logger.debug(f"params = {params}")
        logger.debug(f"results = {results}")

    if len(results) > 1:
        raise DatabaseError("Multiple matching records found in database")

    if len(results) == 1:
        hostname = results[0][1]
        idrac_host = build_idrac_hostname(hostname)
        community_string = os.getenv("SNMP_COMMUNITY", "public")
        BIOS_OID = "1.3.6.1.4.1.674.10892.5.4.300.50.1.8.1.1"
        IDRAC_FW_OID = "1.3.6.1.4.1.674.10892.5.1.1.8.0"

        if idrac:
            idrac_version = await get_snmp_value(
                idrac_host, community_string, IDRAC_FW_OID
            )
        else:
            idrac_version = results[0][3]
        if bios:
            bios_version = await get_snmp_value(idrac_host, community_string, BIOS_OID)
        else:
            bios_version = results[0][4]
        if not model:
            model = results[0][2]
        exp_date = results[0][5]
        exp_epoch = results[0][6]
        conn = sqlite3.connect(warranty)
        cursor = conn.cursor()
        data = {
            "svc_tag": results[0][0],
            "name": results[0][1],
            "model": model,
            "idrac_version": idrac_version,
            "bios_version": bios_version,
            "exp_date": exp_date,
            "exp_epoch": exp_epoch,
        }
        # Insert data
        cursor.execute(
            """
            INSERT OR REPLACE INTO systems
            VALUES (:svc_tag, :name, :model,
            :idrac_version, :bios_version,
            :exp_date, :exp_epoch)
        """,
            data,
        )
        conn.commit()
        conn.close()
        if debug_output:
            logger.info("Database updated successfully")
    else:
        raise DatabaseError("Record not found in database")
    return


async def lookup_dell_warranty(
    service_tag: Optional[str],
    hostname: Optional[str],
    idrac: bool,
    bios: bool,
    full: bool,
    warranty: str,
) -> None:
    """
    Logic for the 'lookup' command. Retrieves a single system's data from
    the DB and prints it to the console in dictionary format.
    """
    db_initialize(warranty)
    conn = sqlite3.connect(warranty)
    cursor = conn.cursor()
    if service_tag:
        query = """
            SELECT * FROM systems
            WHERE svc_tag = :service_tag
        """
        params = {"service_tag": service_tag}
    if hostname:
        query = """
            SELECT * FROM systems
            WHERE name = :hostname
        """
        params = {"hostname": hostname}
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    if len(results) == 0:
        raise DatabaseError("No matching records found in database")
    if len(results) > 1:
        raise DatabaseError("Multiple matching records found in database")
    if len(results) == 1:
        hostname = results[0][1]
        result = {"hostname": hostname}
        model = results[0][2]
        if idrac or full:
            idrac_version = results[0][3]
            result["idrac_version"] = idrac_version
        if bios or full:
            bios_version = results[0][4]
            result["bios_version"] = bios_version
        result["svc_tag"] = results[0][0]
        if not idrac and not bios:
            result["model"] = model
            result["exp_date"] = results[0][5]
            result["exp_epoch"] = results[0][6]
        print(result)
    else:
        raise DatabaseError("Record not found in database")
    return


async def filter_list_results(
    results: List[Tuple],
    bios_le: Optional[str],
    bios_lt: Optional[str],
    bios_ge: Optional[str],
    bios_gt: Optional[str],
    bios_eq: Optional[str],
    idrac_le: Optional[str],
    idrac_lt: Optional[str],
    idrac_ge: Optional[str],
    idrac_gt: Optional[str],
    idrac_eq: Optional[str],
) -> List[Tuple]:
    """
    Helper function to filter a list of systems based on version comparison.
    Converts version strings (e.g., '2.1.1') into tuples for proper numeric comparison.
    """
    output = []
    # columns are svc_tag,hostname,model,idrac_version,bios_version,exp_string,exp_epoch
    for s in results:
        s_idrac = s[3]
        s_idrac_tuple = tuple(map(int, s_idrac.split(".")))
        s_bios = s[4]
        s_bios_tuple = tuple(map(int, s_bios.split(".")))
        if idrac_le:
            idrac_le_tuple = tuple(map(int, idrac_le.split(".")))
            if s_idrac_tuple <= idrac_le_tuple:
                output.append(s)
        if idrac_lt:
            idrac_lt_tuple = tuple(map(int, idrac_lt.split(".")))
            if s_idrac_tuple < idrac_lt_tuple:
                output.append(s)
        if idrac_ge:
            idrac_ge_tuple = tuple(map(int, idrac_ge.split(".")))
            if s_idrac_tuple >= idrac_ge_tuple:
                output.append(s)
        if idrac_gt:
            idrac_gt_tuple = tuple(map(int, idrac_gt.split(".")))
            if s_idrac_tuple > idrac_gt_tuple:
                output.append(s)
        if idrac_eq:
            idrac_eq_tuple = tuple(map(int, idrac_eq.split(".")))
            if s_idrac_tuple == idrac_eq_tuple:
                output.append(s)
        if bios_le:
            bios_le_tuple = tuple(map(int, bios_le.split(".")))
            if s_bios_tuple <= bios_le_tuple:
                output.append(s)
        if bios_lt:
            bios_lt_tuple = tuple(map(int, bios_lt.split(".")))
            if s_bios_tuple < bios_lt_tuple:
                output.append(s)
        if bios_ge:
            bios_ge_tuple = tuple(map(int, bios_ge.split(".")))
            if s_bios_tuple >= bios_ge_tuple:
                output.append(s)
        if bios_gt:
            bios_gt_tuple = tuple(map(int, bios_gt.split(".")))
            if s_bios_tuple > bios_gt_tuple:
                output.append(s)
        if bios_eq:
            bios_eq_tuple = tuple(map(int, bios_eq.split(".")))
            if s_bios_tuple == bios_eq_tuple:
                output.append(s)

    return output


async def list_dell_warranty(
    service_tag: Optional[str],
    hostname: Optional[str],
    model: Optional[str],
    regex: Optional[str],
    bios_le: Optional[str],
    bios_lt: Optional[str],
    bios_ge: Optional[str],
    bios_gt: Optional[str],
    bios_eq: Optional[str],
    idrac_le: Optional[str],
    idrac_lt: Optional[str],
    idrac_ge: Optional[str],
    idrac_gt: Optional[str],
    idrac_eq: Optional[str],
    expires_in: Optional[str],
    expired: bool,
    printjson: bool,
    host_only: bool,
    warranty: str,
) -> None:
    """
    Logic for the 'list' command. Performs complex SQL queries based on filters
    (model, regex, expiration time) and outputs results in JSON,
    Grid table, or hostname-only format.
    """
    db_initialize(warranty)
    conn = sqlite3.connect(warranty)
    cursor = conn.cursor()
    # default query
    query = """
            SELECT * FROM systems
            WHERE svc_tag LIKE '%'
    """
    params = {}
    if service_tag and hostname:
        raise ValidationError(
            "Cannot specify both --svctag and --target; they are mutually exclusive"
        )
    if service_tag:
        query = """
            SELECT * FROM systems
            WHERE svc_tag = :service_tag
        """
        params = {"service_tag": service_tag}
    if hostname:
        query = """
            SELECT * FROM systems
            WHERE name = :hostname
        """
        params = {"hostname": hostname}

    if hostname or service_tag:
        if model or regex:
            raise ValidationError(
                "Cannot specify --model or --regex when using --svctag or --target"
            )

    if model and regex:
        query = """
            SELECT * from systems
            WHERE name LIKE :regex AND model = :model
        """
        params = {"regex": regex, "model": model}

    if model and not regex:
        query = """
            SELECT * from systems
            WHERE model = :model
        """
        params = {"model": model}

    if not model and regex:
        query = """
            SELECT * from systems
            WHERE name LIKE :regex
        """
        params = {"regex": regex}

    if expires_in:
        current_time = int(time.time())
        future_timestamp = current_time + (int(expires_in) * 86400)
        # Only include systems expiring in the future (not already expired)
        query += "AND exp_epoch > :current_time AND exp_epoch <= :future_timestamp\n"
        params["current_time"] = current_time
        params["future_timestamp"] = future_timestamp

    if expired:
        current_time = int(time.time())
        # Only include systems that have already expired
        query += "AND exp_epoch < :current_time\n"
        params["current_time"] = current_time

    # Always sort by hostname for consistent output
    query += " ORDER BY name"

    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    if (
        bios_le
        or bios_lt
        or bios_ge
        or bios_gt
        or bios_eq
        or idrac_le
        or idrac_lt
        or idrac_ge
        or idrac_gt
        or idrac_eq
    ):
        results = await filter_list_results(
            results,
            bios_le,
            bios_lt,
            bios_ge,
            bios_gt,
            bios_eq,
            idrac_le,
            idrac_lt,
            idrac_ge,
            idrac_gt,
            idrac_eq,
        )
    if host_only:
        # Print only hostnames, one per line
        for row in results:
            print(row[1])  # Index 1 is the hostname (name field)
    elif printjson:
        print(json.dumps(results, indent=4))
    else:
        headers = [
            "Service Tag",
            "Hostname",
            "Model",
            "Firmware",
            "BIOS",
            "Expires",
            "Timestamp",
        ]
        print(tabulate(results, headers=headers, tablefmt="grid"))
    return


async def refresh_dell_warranty(
    service_tag: Optional[str], hostname: Optional[str], warranty: str
) -> None:
    """
    Logic for the 'refresh' command. Refreshes SNMP data (BIOS/iDRAC versions)
    and warranty information from Dell API for an existing system.
    """
    db_initialize(warranty)

    # Query the existing record
    if service_tag:
        results = query_by_service_tag(warranty, service_tag)
    elif hostname:
        results = query_by_hostname(warranty, hostname)
    else:
        raise ValidationError("Either service tag or hostname must be provided")

    if len(results) == 0:
        raise DatabaseError("No matching record found to refresh")
    if len(results) > 1:
        raise DatabaseError("Multiple matching records found in database")

    # Extract existing data
    existing = results[0]
    svc_tag = existing[0]
    name = existing[1]
    model = existing[2]

    logger.info(f"Refreshing data for {svc_tag} ({name})")

    # Fetch fresh SNMP data
    idrac_host = build_idrac_hostname(name)
    community_string = os.getenv("SNMP_COMMUNITY", "public")
    BIOS_OID = "1.3.6.1.4.1.674.10892.5.4.300.50.1.8.1.1"
    IDRAC_FW_OID = "1.3.6.1.4.1.674.10892.5.1.1.8.0"

    bios_version = await get_snmp_value(idrac_host, community_string, BIOS_OID)
    idrac_version = await get_snmp_value(idrac_host, community_string, IDRAC_FW_OID)

    logger.info(f"Updated SNMP values - BIOS: {bios_version}, iDRAC: {idrac_version}")

    # Fetch fresh warranty data from Dell
    logger.info("Fetching updated warranty information from Dell API")
    exp_epoch, exp_date = dell_api_warranty_date(svc_tag)

    logger.info(f"Updated warranty expiration: {exp_date}")

    # Update the database
    upsert_system(
        warranty, svc_tag, name, model, idrac_version, bios_version, exp_date, exp_epoch
    )

    logger.info(f"Successfully refreshed record for {svc_tag}")


async def discover_dell_system(hostname: str, warranty: str) -> Tuple[str, str]:
    """
    Logic for the 'discover' command. Queries a Dell iDRAC interface via SNMP
    to automatically discover the service tag and model information.

    Returns:
        Tuple of (service_tag, model) discovered from the system
    """
    logger.info(f"Discovering system information for {hostname}")

    idrac_host = build_idrac_hostname(hostname)
    community_string = os.getenv("SNMP_COMMUNITY", "public")

    # Dell OIDs for service tag and model
    SERVICE_TAG_OID = ".1.3.6.1.4.1.674.10892.5.1.3.2.0"
    MODEL_OID = ".1.3.6.1.4.1.674.10892.5.1.3.12.0"

    logger.info(f"Querying {idrac_host} for service tag and model")

    # Query service tag
    service_tag = await get_snmp_value(idrac_host, community_string, SERVICE_TAG_OID)
    if not service_tag:
        raise SNMPError(f"Failed to retrieve service tag from {idrac_host}")

    # Query model
    model = await get_snmp_value(idrac_host, community_string, MODEL_OID)
    if not model:
        raise SNMPError(f"Failed to retrieve model from {idrac_host}")

    # Strip "PowerEdge " prefix if present
    if model.startswith("PowerEdge "):
        model = model.replace("PowerEdge ", "")

    logger.info(f"Discovered: Service Tag={service_tag}, Model={model}")

    return (service_tag, model)


async def _discover_single_host(
    hostname: str, warranty: str, auto_add: bool
) -> dict:
    """
    Discovers a single host and optionally adds it to the database.
    Returns a result dict with status information.
    """
    result = {"hostname": hostname, "status": "ok", "error": None}
    try:
        service_tag, model = await discover_dell_system(hostname, warranty)
        result["service_tag"] = service_tag
        result["model"] = model

        if auto_add:
            await add_dell_warranty(service_tag, hostname, model, warranty)
            result["added"] = True
        else:
            result["added"] = False
    except (SNMPError, DracsError) as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.error(f"Failed to discover {hostname}: {e}")

    return result


async def discover_dell_systems_batch(
    hosts: List[str], warranty: str, auto_add: bool
) -> None:
    """
    Discovers multiple hosts concurrently using asyncio.gather.
    Prints a summary table of results.
    """
    tasks = [_discover_single_host(h, warranty, auto_add) for h in hosts]
    results = await asyncio.gather(*tasks)

    succeeded = [r for r in results if r["status"] == "ok"]
    failed = [r for r in results if r["status"] == "error"]

    if succeeded:
        table_data = [
            (r["hostname"], r["service_tag"], r["model"],
             "Added" if r.get("added") else "Discovered")
            for r in succeeded
        ]
        headers = ["Hostname", "Service Tag", "Model", "Status"]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    if failed:
        print(f"\nFailed ({len(failed)}/{len(results)}):")
        for r in failed:
            print(f"  {r['hostname']}: {r['error']}")

    total = len(results)
    print(
        f"\nSummary: {len(succeeded)} succeeded, "
        f"{len(failed)} failed out of {total} hosts"
    )


async def remove_dell_warranty(
    service_tag: Optional[str], hostname: Optional[str], warranty: str
) -> None:
    """
    Logic for the 'remove' command. Deletes a system record from the
    database by service tag or hostname.
    """
    if service_tag:
        if debug_output:
            print(f"service_tag = {service_tag}")
    if hostname:
        if debug_output:
            print(f"hostname = {hostname}")

    db_initialize(warranty)
    conn = sqlite3.connect(warranty)
    cursor = conn.cursor()
    if service_tag:
        query = """
            SELECT * FROM systems
            WHERE svc_tag = :service_tag
        """
        params = {"service_tag": service_tag}
    if hostname:
        query = """
            SELECT * FROM systems
            WHERE name = :hostname
        """
        params = {"hostname": hostname}
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    if len(results) == 0:
        raise DatabaseError("No matching records found in database")
    if len(results) > 1:
        raise DatabaseError("Multiple matching records found in database")
    if len(results) == 1:
        hostname = results[0][1]
        result = {"hostname": hostname}
        result["svc_tag"] = results[0][0]
        service_tag = result["svc_tag"]
        query = """
            DELETE FROM systems
            WHERE svc_tag = :service_tag
        """
        params = {"service_tag": result["svc_tag"]}
        conn = sqlite3.connect(warranty)
        cursor = conn.cursor()
        cursor.execute(query, params)
        if cursor.rowcount == 0:
            print(f"No system found with svctag {service_tag}.")
        else:
            conn.commit()
            print("Record deleted")
        conn.close()
    return
