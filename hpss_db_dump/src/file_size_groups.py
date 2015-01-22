import math
import json

def _get_name(s):
    if s == 0:
        return ""
    if 0 < s < 1024:
        return ("%dKB" % s)
    if 1024 <= s < 1024**2:
        return ("%dMB" % (s/1024))
    if 1024**2 <= s < 1024**3:
        return ("%dGB" % (s/(1024**2)))
    if 1024**3 <= s < 1024**4:
        return ("%dTB" % (s/(1024**3))) 
    if 1024**4 <= s < 1024**5:
        return ("%dPB" % (s/(1024**4))) 
    if 1024**5 <= s < 1024**6:
        return ("%EB" % (s/(1024**5))) 

def get_name_map():
    names = dict()
    names[0] = "0-1KB"
    for i in range (1,40):
        from_d = int(math.pow(2,i-1))
        from_s = _get_name(from_d)
        to_d = int(math.pow(2,i))
        to_s = _get_name(to_d)
        names[i] = "%s-%s" % (from_s, to_s)
    return names

_names = get_name_map()

def get_file_size_group(bytes):
    """
    We are interested in the file size/request distribution.
    The log2 of the bytes will group the size into
    :param bytes:
    :return:
    """
    x = int(bytes)
    if x < 1024:  # less than 1KB
        return 0
    return int(math.log((x/1024), 2))+1

def get_group_name(group_id):
    return _names[group_id]


if __name__ == "__main__":

    names = get_name_map()
    # for i in range(40):
    #     print (i, get_file_size_group_name(i))


    for x in [0, 1000, 1050, 2048, 5000, 8191, 8192,8193, 9000, 19240124, 13942390423904]:
        g = get_file_size_group(x)
        
        print ("%d   (%d) %s" %(x, g, names[g]))

    # print(json.dumps(, indent=2))

    print(get_group_name(12))

