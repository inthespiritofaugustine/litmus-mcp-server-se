from typing import Optional, Any
from config import logger
from utils.auth import get_litmus_connection
from utils.formatting import format_success_response, format_error_response
from .data_tools import get_current_value_on_topic

from mcp.shared.exceptions import McpError
from mcp.types import ErrorData, INVALID_PARAMS, INTERNAL_ERROR
from mcp.types import TextContent
from starlette.requests import Request
from litmussdk.devicehub import devices, tags
from litmussdk.devicehub.tags import Tag
from litmussdk.devicehub.drivers import list_all_drivers


async def get_litmusedge_driver_list(request: Request) -> list[TextContent]:
    """
    Retrieves all available drivers supported by Litmus Edge DeviceHub.

    Returns a list of supported industrial protocols and device drivers
    (e.g., ModbusTCP, OPCUA, BACnet, MQTT).
    """
    try:

        connection = get_litmus_connection(request)
        driver_list = list_all_drivers(le_connection=connection)

        drivers = []
        for driver in driver_list:
            driver_info = {
                "name": driver.name,
                "id": getattr(driver, "id", None),
                "protocol": getattr(driver, "protocol", None),
                "version": getattr(driver, "version", None),
                "description": getattr(driver, "description", None),
                "category": getattr(driver, "category", None),
            }
            drivers.append({k: v for k, v in driver_info.items() if v is not None})

        drivers.sort(key=lambda x: x["name"])

        logger.info(f"Retrieved {len(drivers)} drivers from Litmus Edge")

        result = {
            "count": len(drivers),
            "drivers": drivers,
            "driver_names": [d["name"] for d in drivers],
        }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error retrieving driver list: {e}", exc_info=True)
        return format_error_response("retrieval_failed", str(e), count=0, drivers=[])


async def get_devicehub_devices(request: Request, arguments: dict) -> list[TextContent]:
    """
    Retrieves all configured devices in the DeviceHub module.

    Supports filtering by driver type and status, with optional tag inclusion.
    """
    try:
        filter_by_driver = arguments.get("filter_by_driver")

        connection = get_litmus_connection(request)
        device_list = devices.list_devices(le_connection=connection)
        logger.info(f"Retrieved {len(device_list)} devices from Litmus Edge")

        device_data = []
        for current_device in device_list:
            device_info = _build_device_info(current_device)

            # Apply filters
            if filter_by_driver and device_info.get("driver") != filter_by_driver:
                continue
            device_data.append(device_info)

        device_data.sort(key=lambda x: x["name"])

        logger.info(f"Retrieved {len(device_data)} devices from Litmus Edge")

        summary = _create_device_summary(device_data)

        result = {
            "count": len(device_data),
            "devices": device_data,
            "summary": summary,
        }

        if filter_by_driver:
            result["filters_applied"] = {
                "driver": filter_by_driver,
            }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error retrieving devices: {e}", exc_info=True)
        return format_error_response("retrieval_failed", str(e), count=0, devices=[])


async def create_devicehub_device(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Creates a new device in DeviceHub with specified driver.

    IMPORTANT: Creates device with default settings. You'll need to:
    1. Update connection properties (IP, port, slave ID, etc.)
    2. Configure tags/registers
    3. Enable the device
    """
    try:
        name = arguments.get("name")
        selected_driver = arguments.get("selected_driver")

        if not name:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'name' parameter is required")
            )
        if not selected_driver:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message="'selected_driver' parameter is required",
                )
            )

        connection = get_litmus_connection(request)

        # Get driver information
        driver_list = list_all_drivers(le_connection=connection)
        driver_map = {}
        driver_names = []

        for driver in driver_list:
            driver_map[driver.name] = {
                "id": driver.id,
                "properties": driver.get_default_properties(),
            }
            driver_names.append(driver.name)

        if selected_driver not in driver_names:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Driver '{selected_driver}' not found. Available drivers: {driver_names}",
                )
            )

        # Create device
        device = devices.Device(
            name=name,
            properties=driver_map[selected_driver]["properties"],
            driver=driver_map[selected_driver]["id"],
        )

        created_device = devices.create_device(device, le_connection=connection)

        device_dict = (
            created_device.__dict__
            if hasattr(created_device, "__dict__")
            else {"id": str(created_device)}
        )

        logger.info(f"Created device '{name}' with driver '{selected_driver}'")

        result = {
            "device": device_dict,
            "next_steps": [
                "Update connection properties (IP address, port, etc.)",
                "Configure tags/registers for data collection",
                "Enable the device to start communication",
            ],
        }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error creating device: {e}", exc_info=True)
        return format_error_response("creation_failed", str(e))


async def get_devicehub_device_tags(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Retrieves all tags (data points/registers) for a specific device.

    Returns tag configuration including address, data type, scaling, etc.
    """
    try:
        device_name = arguments.get("device_name")

        if not device_name:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS, message="'device_name' parameter is required"
                )
            )

        connection = get_litmus_connection(request)

        # Find the device
        requested_device = _find_device_by_name(connection, device_name)
        if not requested_device:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Device '{device_name}' not found. Use get_devicehub_devices to see available devices.",
                )
            )

        # Get tags
        tag_list = tags.list_registers_from_single_device(requested_device)

        tag_data = []
        for current_tag in tag_list:
            tag_info = {
                "tag_name": current_tag.tag_name,
                "id": getattr(current_tag, "id", None),
                "address": getattr(current_tag, "address", None),
                "data_type": getattr(current_tag, "data_type", None),
                "scaling": getattr(current_tag, "scaling", None),
                "read_write": getattr(current_tag, "read_write", None),
                "unit": getattr(current_tag, "unit", None),
                "description": getattr(current_tag, "description", None),
            }
            tag_data.append({k: v for k, v in tag_info.items() if v is not None})

        tag_data.sort(key=lambda x: x["tag_name"])

        logger.info(f"Retrieved {len(tag_data)} tags for device '{device_name}'")

        result = {
            "device_name": device_name,
            "count": len(tag_data),
            "tags": tag_data,
            "tag_names": [t["tag_name"] for t in tag_data],
        }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error retrieving tags: {e}", exc_info=True)
        return format_error_response("retrieval_failed", str(e), count=0, tags=[])


async def get_current_value_of_devicehub_tag(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Reads the current real-time value of a specific tag from a device.

    Requires either tag_name OR tag_id (not both).
    """
    try:
        device_name = arguments.get("device_name")
        tag_name = arguments.get("tag_name")
        tag_id = arguments.get("tag_id")

        if not device_name:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS, message="'device_name' parameter is required"
                )
            )

        if not tag_name and not tag_id:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message="Either 'tag_name' or 'tag_id' is required. Use get_devicehub_device_tags to see available tags.",
                )
            )

        connection = get_litmus_connection(request)

        # Find device
        requested_device = _find_device_by_name(connection, device_name)
        if not requested_device:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Device '{device_name}' not found. Use get_devicehub_devices to see available devices.",
                )
            )

        # Find tag
        tag_list = tags.list_registers_from_single_device(requested_device)

        if tag_name:
            requested_tag = next(
                (tag for tag in tag_list if tag.tag_name == tag_name), None
            )
            identifier = f"name '{tag_name}'"
        else:
            requested_tag = next((tag for tag in tag_list if tag.id == tag_id), None)
            identifier = f"ID '{tag_id}'"

        if not requested_tag:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Tag with {identifier} not found on device '{device_name}'",
                )
            )

        # Get the output topic
        requested_value_from_topic = next(
            (
                topic.topic
                for topic in requested_tag.topics
                if topic.direction == "Output"
            ),
            None,
        )

        if not requested_value_from_topic:
            raise McpError(
                ErrorData(
                    code=INTERNAL_ERROR,
                    message=f"No output topic found for tag {identifier}",
                )
            )

        # Read current value
        value_data = await get_current_value_on_topic(
            topic=requested_value_from_topic, request=request
        )

        logger.info(f"Read value for {identifier} on device '{device_name}'")

        result = {
            "device_name": device_name,
            "tag_name": tag_name or requested_tag.tag_name,
            "tag_id": tag_id or requested_tag.id,
            "data": value_data,
        }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error reading tag value: {e}", exc_info=True)
        return format_error_response("read_failed", str(e))


def _find_device_by_name(connection: Any, device_name: str) -> Optional[Any]:
    """Find a device by name from the device list."""
    device_list = devices.list_devices(le_connection=connection)
    for device in device_list:
        if device.name == device_name:
            return device
    return None


def _build_device_info(device: Any) -> dict:
    """Build device information dictionary."""
    device_info = {
        "name": device.name,
        "id": getattr(device, "id", None),
        "driver": getattr(device, "driver", None),
        "metadata": getattr(device, "metadata", "unknown"),
        "description": getattr(device, "description", None),
        "properties": getattr(device, "properties", None),
    }

    device_info = {k: v for k, v in device_info.items() if v is not None}

    return device_info


def _create_device_summary(device_data: list[dict]) -> dict:
    """Create summary statistics for devices."""
    driver_counts = {}

    for device in device_data:
        driver = device.get("driver", "unknown")
        driver_counts[driver] = driver_counts.get(driver, 0) + 1

    return {
        "by_driver": driver_counts,
    }


async def list_all_devicehub_tags(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Lists all tags across the entire Litmus Edge instance (up to 1000).

    Optionally filter by device name.
    """
    try:
        device_name_filter = arguments.get("device_name")

        connection = get_litmus_connection(request)

        # Get all tags
        all_tags = tags.list_all_tags(le_connection=connection)

        tag_data = []
        device_counts = {}

        for current_tag in all_tags:
            # Extract device info
            device_id = getattr(current_tag, "device", None)

            # Apply device filter if specified
            if device_name_filter:
                # We need to check if this tag belongs to the specified device
                # The device field may be an ID, so we need to handle this
                tag_device_name = getattr(current_tag, "device_name", None)
                if tag_device_name and tag_device_name != device_name_filter:
                    continue

            tag_info = {
                "tag_name": current_tag.tag_name,
                "id": getattr(current_tag, "id", None),
                "device": device_id,
                "value_type": getattr(current_tag, "value_type", None),
                "description": getattr(current_tag, "description", None),
                "publish_cov": getattr(current_tag, "publish_cov", None),
            }
            tag_data.append({k: v for k, v in tag_info.items() if v is not None})

            # Count by device
            device_key = device_id or "unknown"
            device_counts[device_key] = device_counts.get(device_key, 0) + 1

        tag_data.sort(key=lambda x: (x.get("device", ""), x.get("tag_name", "")))

        logger.info(f"Retrieved {len(tag_data)} tags from Litmus Edge")

        result = {
            "count": len(tag_data),
            "tags": tag_data,
            "summary": {"by_device": device_counts},
        }

        if device_name_filter:
            result["filters_applied"] = {"device_name": device_name_filter}

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error retrieving all tags: {e}", exc_info=True)
        return format_error_response("retrieval_failed", str(e), count=0, tags=[])


async def create_devicehub_tag(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Creates a new tag (data point/register) on a device.

    Requires device name, tag name, and value type.
    """
    try:
        device_name = arguments.get("device_name")
        tag_name = arguments.get("tag_name")
        value_type = arguments.get("value_type")
        description = arguments.get("description", "")
        properties = arguments.get("properties", [])
        publish_cov = arguments.get("publish_cov", False)

        # Validate required parameters
        if not device_name:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'device_name' parameter is required")
            )
        if not tag_name:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'tag_name' parameter is required")
            )
        if not value_type:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'value_type' parameter is required")
            )

        connection = get_litmus_connection(request)

        # Find the device
        requested_device = _find_device_by_name(connection, device_name)
        if not requested_device:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Device '{device_name}' not found. Use get_devicehub_devices to see available devices.",
                )
            )

        # Create the tag object
        new_tag = Tag(
            device=requested_device.id,
            tag_name=tag_name,
            value_type=value_type,
            description=description,
            properties=properties,
            publish_cov=publish_cov,
        )

        # Create the tag
        created_tags = tags.create_tags([new_tag], le_connection=connection)

        if created_tags and len(created_tags) > 0:
            created_tag = created_tags[0]
            tag_info = {
                "tag_name": created_tag.tag_name,
                "id": getattr(created_tag, "id", None),
                "device": getattr(created_tag, "device", None),
                "value_type": getattr(created_tag, "value_type", None),
                "description": getattr(created_tag, "description", None),
                "publish_cov": getattr(created_tag, "publish_cov", None),
            }
            tag_info = {k: v for k, v in tag_info.items() if v is not None}

            logger.info(f"Created tag '{tag_name}' on device '{device_name}'")

            result = {
                "device_name": device_name,
                "tag": tag_info,
            }

            return format_success_response(result)
        else:
            raise McpError(
                ErrorData(
                    code=INTERNAL_ERROR,
                    message="Tag creation returned no results",
                )
            )

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error creating tag: {e}", exc_info=True)
        return format_error_response("creation_failed", str(e))


async def update_devicehub_tag(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Updates an existing tag's configuration.

    Requires device_name and tag_id, plus fields to update.
    """
    try:
        device_name = arguments.get("device_name")
        tag_id = arguments.get("tag_id")
        new_tag_name = arguments.get("tag_name")
        new_value_type = arguments.get("value_type")
        new_description = arguments.get("description")
        new_properties = arguments.get("properties")
        new_publish_cov = arguments.get("publish_cov")

        # Validate required parameters
        if not device_name:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'device_name' parameter is required")
            )
        if not tag_id:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'tag_id' parameter is required for updates")
            )

        connection = get_litmus_connection(request)

        # Find the device
        requested_device = _find_device_by_name(connection, device_name)
        if not requested_device:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Device '{device_name}' not found. Use get_devicehub_devices to see available devices.",
                )
            )

        # Find the existing tag
        tag_list = tags.list_registers_from_single_device(requested_device)
        existing_tag = next((tag for tag in tag_list if tag.id == tag_id), None)

        if not existing_tag:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Tag with ID '{tag_id}' not found on device '{device_name}'",
                )
            )

        # Update fields if provided
        if new_tag_name is not None:
            existing_tag.tag_name = new_tag_name
        if new_value_type is not None:
            existing_tag.value_type = new_value_type
        if new_description is not None:
            existing_tag.description = new_description
        if new_properties is not None:
            existing_tag.properties = new_properties
        if new_publish_cov is not None:
            existing_tag.publish_cov = new_publish_cov

        # Update the tag
        updated_tags = tags.update_tags([existing_tag], le_connection=connection)

        if updated_tags and len(updated_tags) > 0:
            updated_tag = updated_tags[0]
            tag_info = {
                "tag_name": updated_tag.tag_name,
                "id": getattr(updated_tag, "id", None),
                "device": getattr(updated_tag, "device", None),
                "value_type": getattr(updated_tag, "value_type", None),
                "description": getattr(updated_tag, "description", None),
                "publish_cov": getattr(updated_tag, "publish_cov", None),
            }
            tag_info = {k: v for k, v in tag_info.items() if v is not None}

            logger.info(f"Updated tag '{tag_id}' on device '{device_name}'")

            result = {
                "device_name": device_name,
                "tag": tag_info,
            }

            return format_success_response(result)
        else:
            raise McpError(
                ErrorData(
                    code=INTERNAL_ERROR,
                    message="Tag update returned no results",
                )
            )

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error updating tag: {e}", exc_info=True)
        return format_error_response("update_failed", str(e))


async def delete_devicehub_tag(
    request: Request, arguments: dict
) -> list[TextContent]:
    """
    Deletes one or more tags from a device.

    Provide tag_name, tag_id, or tag_ids (list) to specify which tags to delete.
    """
    try:
        device_name = arguments.get("device_name")
        tag_name = arguments.get("tag_name")
        tag_id = arguments.get("tag_id")
        tag_ids = arguments.get("tag_ids", [])

        # Validate required parameters
        if not device_name:
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message="'device_name' parameter is required")
            )

        if not tag_name and not tag_id and not tag_ids:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message="Either 'tag_name', 'tag_id', or 'tag_ids' is required",
                )
            )

        connection = get_litmus_connection(request)

        # Find the device
        requested_device = _find_device_by_name(connection, device_name)
        if not requested_device:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message=f"Device '{device_name}' not found. Use get_devicehub_devices to see available devices.",
                )
            )

        # Get all tags for the device
        tag_list = tags.list_registers_from_single_device(requested_device)

        # Find tags to delete
        tags_to_delete = []
        deleted_tag_names = []

        if tag_ids:
            # Batch delete by IDs
            for tid in tag_ids:
                found_tag = next((t for t in tag_list if t.id == tid), None)
                if found_tag:
                    tags_to_delete.append(found_tag)
                    deleted_tag_names.append(found_tag.tag_name)
        elif tag_id:
            # Single delete by ID
            found_tag = next((t for t in tag_list if t.id == tag_id), None)
            if found_tag:
                tags_to_delete.append(found_tag)
                deleted_tag_names.append(found_tag.tag_name)
        elif tag_name:
            # Single delete by name
            found_tag = next((t for t in tag_list if t.tag_name == tag_name), None)
            if found_tag:
                tags_to_delete.append(found_tag)
                deleted_tag_names.append(found_tag.tag_name)

        if not tags_to_delete:
            raise McpError(
                ErrorData(
                    code=INVALID_PARAMS,
                    message="No matching tags found to delete",
                )
            )

        # Delete the tags
        if len(tags_to_delete) == 1:
            tags.delete_tag(tags_to_delete[0], le_connection=connection)
        else:
            tags.delete_tags(tags_to_delete, le_connection=connection)

        logger.info(f"Deleted {len(tags_to_delete)} tag(s) from device '{device_name}'")

        result = {
            "device_name": device_name,
            "deleted_count": len(tags_to_delete),
            "deleted_tags": deleted_tag_names,
        }

        return format_success_response(result)

    except McpError:
        raise
    except Exception as e:
        logger.error(f"Error deleting tag(s): {e}", exc_info=True)
        return format_error_response("deletion_failed", str(e))
