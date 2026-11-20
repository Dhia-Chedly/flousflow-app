import json

MONEY = ("amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest")


def json_type_error(raw):
    try:
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError('duplicate JSON key')
                result[key] = value
            return result
        def bad_constant(value):
            raise ValueError('non-finite number')
        event = json.loads(bytes(raw).decode('utf-8'), object_pairs_hook=pairs, parse_constant=bad_constant)
    except (ValueError, UnicodeDecodeError, TypeError):
        return 'MALFORMED_JSON'
    if not isinstance(event, dict):
        return 'INVALID_FIELD_TYPE'
    for name in ('source_row_number',):
        if name in event and type(event[name]) is not int:
            return 'INVALID_FIELD_TYPE'
    for name in ('source_dataset_id', 'transaction_id', 'replay_id', 'event_id', 'event_time', 'produced_at'):
        if name in event and event[name] is not None and not isinstance(event[name], str):
            return 'INVALID_FIELD_TYPE'
    payload = event.get('payload')
    if payload is not None and not isinstance(payload, dict):
        return 'INVALID_FIELD_TYPE'
    if isinstance(payload, dict):
        for name in ('step', 'isFraud', 'isFlaggedFraud'):
            if name in payload and type(payload[name]) is not int:
                return 'INVALID_FIELD_TYPE'
        for name in ('type', 'nameOrig', 'nameDest', *MONEY):
            if name in payload and payload[name] is not None and not isinstance(payload[name], str):
                return 'INVALID_FIELD_TYPE'
    return None

