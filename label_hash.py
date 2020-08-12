

def ts_hash(all_labels=None, metric_name=None, label_config=None):
    """[Compute unique hash for TS]
    Args:
        all_labels ([dict]): [combined label_config with __name__ label]
        metric_name ([string], optional): [label name. If provided added as __name__ label]. Defaults to None.
        label_config ([dict], optional): [label_config without __name__]. Defaults to None.

    Returns:
        [int]: [unique TS hash]
    """
    hash_dict = {}
    if all_labels is not None:
        hash_dict.update(all_labels)
    if metric_name is not None:
        hash_dict.update({"__name__": metric_name})
    if label_config is not None:
        hash_dict.update(label_config)
    return hash(frozenset(hash_dict.items()))