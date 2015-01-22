import hashlib

def get_md5(s, hexdigest=False):
    # return s
    m = hashlib.md5()
    m.update(s.encode())
    if hexdigest:
        return m.hexdigest()
    else:
        return m.digest()