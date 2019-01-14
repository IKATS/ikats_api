from ikats import IkatsAPI
from ikats.exceptions import IkatsNotFoundError


def delete_ts_if_exists(fid):
    """
    Delete a TS if it exists
    Nothing is return
    Useful to prepare environments

    :param fid: FID of the TS to delete
    """
    api = IkatsAPI()

    try:
        ts = api.ts.get(fid=fid)
        return api.ts.delete(ts=ts, raise_exception=False)
    except IkatsNotFoundError:
        return True
